//! DCC (Direct Client-to-Client) transport.
//!
//! Like the IRC client in this crate, this is a byte pipe: it opens or accepts
//! the peer socket, optionally wraps it in TLS for the secure variants, and
//! then either pumps newline-framed chat or moves a file. It knows nothing
//! about CTCP — the caller parses the offer and decides whether to act on it.
//!
//! Two roles:
//!   - **offerer** — [`DccSession::listen`] binds a port so the caller can put
//!     it in a `DCC CHAT`/`DCC SEND` CTCP, then waits for the peer to connect.
//!   - **acceptor** — [`DccSession::connect`] dials the address from an offer.
//!
//! Security properties that live here rather than in the caller: the listener
//! only accepts a connection from the address the offer went to, both roles
//! give up if the peer never shows, and a transfer whose byte count disagrees
//! with the announced size fails instead of leaving a plausible-looking but
//! corrupt file on disk.

mod chat;
mod listener;
mod stream;
mod transfer;

use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;

use crate::client::Encoding;

pub use listener::DccListener;

/// How long to wait for the peer to connect to a port we advertised.
pub const ACCEPT_TIMEOUT: Duration = Duration::from_secs(120);

/// How long to wait when dialling a peer's advertised address.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum DccError {
    #[error("no free port in the configured range")]
    NoFreePort,

    #[error("timed out waiting for the peer")]
    Timeout,

    #[error("connection from unexpected address {0}")]
    UnexpectedPeer(IpAddr),

    #[error("transfer size mismatch: expected {expected} bytes, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },

    #[error("receive buffer overflow: peer sent too much data without line terminators")]
    BufferOverflow,

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Debug)]
pub enum DccEvent {
    /// A port was bound and is in the CTCP offer the caller is about to send.
    Listening { port: u16 },
    /// The peer socket is up. `tls_fingerprint` is present only on the dialling
    /// side of a secure session (see `stream.rs`).
    Connected { tls_fingerprint: Option<String> },
    /// One line of DCC CHAT text from the peer.
    Line { text: String },
    /// Bytes moved so far, for a file transfer.
    Progress { transferred: u64 },
    /// The transfer finished and, when receiving, the file is on disk at `path`.
    Completed { path: Option<String> },
    /// The session ended. Always the last event.
    Closed,
    Error(String),
}

#[derive(Debug)]
pub enum DccCommand {
    SendLine(String),
    Close,
}

#[derive(Clone, Debug)]
pub struct DccListenOptions {
    pub secure: bool,
    /// Inclusive port range to bind inside. `0..=0` means "any free port".
    pub port_start: u16,
    pub port_end: u16,
    /// Only accept a connection from this address.
    pub expect_peer: Option<IpAddr>,
    /// `Some` for DCC SEND (we transmit this file), `None` for DCC CHAT.
    pub file_path: Option<PathBuf>,
    pub encoding: Encoding,
}

#[derive(Clone, Debug)]
pub struct DccConnectOptions {
    pub host: String,
    pub port: u16,
    pub secure: bool,
    /// `Some` when receiving a file, `None` for DCC CHAT.
    pub save_path: Option<PathBuf>,
    /// Announced size, used to detect a short or over-long transfer.
    pub size: Option<u64>,
    pub encoding: Encoding,
}

/// Handle to a running DCC session.
#[derive(Clone)]
pub struct DccSession {
    cmd_tx: mpsc::Sender<DccCommand>,
}

impl DccSession {
    /// Bind a port, then wait for the peer on a background task.
    ///
    /// Returns as soon as the socket is bound, so the caller can put the real
    /// port into the CTCP offer before anyone could connect to it.
    pub fn listen(
        options: DccListenOptions,
    ) -> Result<(Self, u16, mpsc::Receiver<DccEvent>), DccError> {
        let listener = DccListener::bind(options.port_start, options.port_end)?;
        let port = listener.port();

        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::channel(256);

        tokio::spawn(async move {
            let _ = event_tx.send(DccEvent::Listening { port }).await;
            let result = run_listen(listener, options, cmd_rx, &event_tx).await;
            finish(result, &event_tx).await;
        });

        Ok((Self { cmd_tx }, port, event_rx))
    }

    /// Dial the address from a peer's offer.
    pub fn connect(options: DccConnectOptions) -> (Self, mpsc::Receiver<DccEvent>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::channel(256);

        tokio::spawn(async move {
            let result = run_connect(options, cmd_rx, &event_tx).await;
            finish(result, &event_tx).await;
        });

        (Self { cmd_tx }, event_rx)
    }

    pub async fn send_line(&self, text: impl Into<String>) -> Result<(), DccError> {
        self.cmd_tx
            .send(DccCommand::SendLine(text.into()))
            .await
            .map_err(|_| {
                DccError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "dcc session gone",
                ))
            })
    }

    pub async fn close(&self) -> Result<(), DccError> {
        // A session that already ended is not an error to close.
        let _ = self.cmd_tx.send(DccCommand::Close).await;
        Ok(())
    }
}

async fn finish(result: Result<Option<String>, DccError>, events: &mpsc::Sender<DccEvent>) {
    match result {
        Ok(path) => {
            let _ = events.send(DccEvent::Completed { path }).await;
        }
        Err(e) => {
            let _ = events.send(DccEvent::Error(e.to_string())).await;
        }
    }
    let _ = events.send(DccEvent::Closed).await;
}

/// Wrap an accepted/dialled socket in TLS when the session is a secure variant.
async fn wrap(
    tcp: TcpStream,
    secure: bool,
    incoming: bool,
) -> Result<stream::DccStream, DccError> {
    if !secure {
        return Ok(stream::plain(tcp));
    }
    if incoming {
        stream::accept_tls(tcp).await
    } else {
        stream::connect_tls(tcp).await
    }
}

async fn run_listen(
    listener: DccListener,
    options: DccListenOptions,
    commands: mpsc::Receiver<DccCommand>,
    events: &mpsc::Sender<DccEvent>,
) -> Result<Option<String>, DccError> {
    let tcp = listener
        .accept_from(options.expect_peer, ACCEPT_TIMEOUT)
        .await?;

    let stream = wrap(tcp, options.secure, true).await?;
    let _ = events
        .send(DccEvent::Connected {
            tls_fingerprint: stream.fingerprint.clone(),
        })
        .await;

    match options.file_path {
        // Offering a file: we are the sender.
        Some(path) => {
            transfer::send_file(stream, &path, events).await?;
            Ok(None)
        }
        None => {
            chat::run_chat(stream, commands, events, options.encoding).await?;
            Ok(None)
        }
    }
}

async fn run_connect(
    options: DccConnectOptions,
    commands: mpsc::Receiver<DccCommand>,
    events: &mpsc::Sender<DccEvent>,
) -> Result<Option<String>, DccError> {
    let addr = format!("{}:{}", options.host, options.port);
    let tcp = time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr))
        .await
        .map_err(|_| DccError::Timeout)??;

    let stream = wrap(tcp, options.secure, false).await?;
    let _ = events
        .send(DccEvent::Connected {
            tls_fingerprint: stream.fingerprint.clone(),
        })
        .await;

    match options.save_path {
        // Accepting a file: we are the receiver.
        Some(path) => {
            transfer::receive_file(stream, &path, options.size, events).await?;
            Ok(Some(path.to_string_lossy().into_owned()))
        }
        None => {
            chat::run_chat(stream, commands, events, options.encoding).await?;
            Ok(None)
        }
    }
}
