//! DCC (Direct Client-to-Client) as a byte pipe.
//!
//! Opens or accepts the peer socket, optionally wraps it in TLS, then either carries newline-framed
//! chat or moves a file. CTCP offers are parsed by the caller.
//!
//! - offerer: [`DccSession::listen`] binds a port for the caller's `DCC CHAT`/`DCC SEND` offer.
//! - acceptor: [`DccSession::connect`] dials the address from a peer's offer.

mod chat;
mod listener;
mod stream;
mod transfer;

use std::future::Future;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time;

use crate::codec::Encoding;
use listener::DccListener;

/// How long a port we advertised waits for the peer.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(120);

/// How long dialling a peer's advertised address may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum DccError {
    #[error("no free port in the configured range")]
    NoFreePort,

    #[error("timed out waiting for the peer")]
    Timeout,

    #[error("session closed before it finished")]
    Cancelled,

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
    /// The port is bound and can go into the offer.
    Listening {
        port: u16,
    },
    /// `tls_fingerprint` is known only on the dialling side of a secure session (see `stream.rs`).
    Connected {
        tls_fingerprint: Option<String>,
    },
    /// One line of DCC CHAT text from the peer.
    Line {
        text: String,
    },
    Progress {
        transferred: u64,
    },
    /// The session finished; `path` is the received file, if any.
    Completed {
        path: Option<String>,
    },
    /// Reported just before `Closed`.
    Error(String),
    /// The session ended. Always the last event.
    Closed,
}

#[derive(Clone, Debug)]
pub struct DccListenOptions {
    pub secure: bool,
    /// Inclusive port range to bind in; `0..=0` means any free port.
    pub port_start: u16,
    pub port_end: u16,
    /// Only accept a connection from this address.
    pub expect_peer: Option<IpAddr>,
    /// The file to send for DCC SEND; `None` for DCC CHAT.
    pub file_path: Option<PathBuf>,
    pub encoding: Encoding,
}

#[derive(Clone, Debug)]
pub struct DccConnectOptions {
    pub host: String,
    pub port: u16,
    pub secure: bool,
    /// Where to save the offered file; `None` for DCC CHAT.
    pub save_path: Option<PathBuf>,
    /// The announced size, used to reject a short or over-long transfer.
    pub size: Option<u64>,
    pub encoding: Encoding,
}

#[derive(Debug)]
enum ChatCommand {
    SendLine(String),
    Close,
}

/// Handle to a running DCC session. Dropping every clone closes it.
#[derive(Clone)]
pub struct DccSession {
    // Chat lines and the close that follows them must stay in order, so chat has its own queue
    chat: mpsc::Sender<ChatCommand>,
    // Interrupts the stages that read no commands: waiting for the peer, the TLS handshake, a transfer
    closed: Arc<watch::Sender<bool>>,
}

/// Receiving ends of a [`DccSession`], owned by its task.
struct SessionControl {
    chat: mpsc::Receiver<ChatCommand>,
    closed: watch::Receiver<bool>,
}

impl SessionControl {
    /// Runs `stage` unless the session is closed first.
    async fn unless_closed<T>(
        &mut self,
        stage: impl Future<Output = Result<T, DccError>>,
    ) -> Result<T, DccError> {
        tokio::select! {
            result = stage => result,
            // Also resolves when every handle is dropped
            _ = self.closed.wait_for(|&closed| closed) => Err(DccError::Cancelled),
        }
    }
}

impl DccSession {
    fn new() -> (Self, SessionControl) {
        let (chat_tx, chat_rx) = mpsc::channel(64);
        let (closed_tx, closed_rx) = watch::channel(false);
        let session = Self {
            chat: chat_tx,
            closed: Arc::new(closed_tx),
        };
        let control = SessionControl {
            chat: chat_rx,
            closed: closed_rx,
        };
        (session, control)
    }

    /// Binds a port, then waits for the peer in the background.
    ///
    /// Returns once the port is bound, so the caller can put it into the offer before anyone connects.
    pub fn listen(
        options: DccListenOptions,
    ) -> Result<(Self, u16, mpsc::Receiver<DccEvent>), DccError> {
        let listener = DccListener::bind(options.port_start, options.port_end)?;
        let port = listener.port();
        let (session, control) = Self::new();
        let (event_tx, event_rx) = mpsc::channel(256);

        tokio::spawn(async move {
            let _ = event_tx.send(DccEvent::Listening { port }).await;
            let result = run_listen(listener, options, control, &event_tx).await;
            finish(result, &event_tx).await;
        });

        Ok((session, port, event_rx))
    }

    /// Dials the address from a peer's offer in the background.
    pub fn connect(options: DccConnectOptions) -> (Self, mpsc::Receiver<DccEvent>) {
        let (session, control) = Self::new();
        let (event_tx, event_rx) = mpsc::channel(256);

        tokio::spawn(async move {
            let result = run_connect(options, control, &event_tx).await;
            finish(result, &event_tx).await;
        });

        (session, event_rx)
    }

    pub async fn send_line(&self, text: impl Into<String>) -> Result<(), DccError> {
        self.chat
            .send(ChatCommand::SendLine(text.into()))
            .await
            .map_err(|_| DccError::Cancelled)
    }

    /// Ends the session; chat lines already sent are delivered first. Closing an ended session is a no-op.
    pub async fn close(&self) {
        let _ = self.chat.send(ChatCommand::Close).await;
        self.closed.send_replace(true);
    }
}

async fn finish(result: Result<Option<PathBuf>, DccError>, events: &mpsc::Sender<DccEvent>) {
    let event = match result {
        Ok(path) => DccEvent::Completed {
            path: path.map(|p| p.to_string_lossy().into_owned()),
        },
        Err(error) => DccEvent::Error(error.to_string()),
    };
    let _ = events.send(event).await;
    let _ = events.send(DccEvent::Closed).await;
}

async fn run_listen(
    listener: DccListener,
    options: DccListenOptions,
    mut control: SessionControl,
    events: &mpsc::Sender<DccEvent>,
) -> Result<Option<PathBuf>, DccError> {
    let tcp = control
        .unless_closed(listener.accept_from(options.expect_peer, ACCEPT_TIMEOUT))
        .await?;
    let stream = if options.secure {
        control.unless_closed(stream::accept_tls(tcp)).await?
    } else {
        stream::plain(tcp)
    };
    report_connected(stream.fingerprint.clone(), events).await;

    match options.file_path {
        Some(path) => {
            control
                .unless_closed(transfer::send_file(stream, &path, events))
                .await?
        }
        None => chat::run_chat(stream, &mut control.chat, events, options.encoding).await?,
    }
    Ok(None)
}

async fn run_connect(
    options: DccConnectOptions,
    mut control: SessionControl,
    events: &mpsc::Sender<DccEvent>,
) -> Result<Option<PathBuf>, DccError> {
    let dial = async {
        time::timeout(
            CONNECT_TIMEOUT,
            TcpStream::connect((options.host.as_str(), options.port)),
        )
        .await
        .map_err(|_| DccError::Timeout)?
        .map_err(DccError::Io)
    };
    let tcp = control.unless_closed(dial).await?;
    let stream = if options.secure {
        control.unless_closed(stream::connect_tls(tcp)).await?
    } else {
        stream::plain(tcp)
    };
    report_connected(stream.fingerprint.clone(), events).await;

    let Some(path) = options.save_path else {
        chat::run_chat(stream, &mut control.chat, events, options.encoding).await?;
        return Ok(None);
    };

    let received = control
        .unless_closed(transfer::receive_file(stream, &path, options.size, events))
        .await;
    if received.is_err() {
        // A partial download would look complete on disk
        let _ = tokio::fs::remove_file(&path).await;
    }
    received?;
    Ok(Some(path))
}

async fn report_connected(tls_fingerprint: Option<String>, events: &mpsc::Sender<DccEvent>) {
    let _ = events.send(DccEvent::Connected { tls_fingerprint }).await;
}
