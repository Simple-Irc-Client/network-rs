//! Async IRC byte-pipe transport.
//!
//! The driver runs as a single tokio task that owns the socket and a `select!`
//! loop over:
//!   - socket reads (line buffered) -> emitted as inbound `Raw` events
//!   - inbound commands from the caller (send / quit / disconnect)
//!
//! This is a **pure transport**: it does NOT speak any IRC protocol — no
//! registration, no CAP negotiation, not even PING/PONG. The caller (the app's
//! kernel) owns the entire IRC conversation, including replying to server PINGs
//! and connection-liveness/keepalive policy. The driver only establishes the
//! TCP/TLS socket, surfaces every received line, writes the lines the caller
//! sends (rate-limited), and reports closure/errors.
//!
//! Events flow out over an `mpsc::Receiver<IrcEvent>` so the consumer can
//! forward them however it likes.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use crate::codec::{strip_crlf, LineBuffer, MAX_RECEIVE_BUFFER};
use crate::error::IrcError;
use crate::ratelimit::SlidingWindowLimiter;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Encoding {
    #[default]
    Utf8,
    Latin1,
}

#[derive(Clone, Debug)]
pub struct IrcClientOptions {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub encoding: Encoding,
}

impl IrcClientOptions {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            tls: false,
            encoding: Encoding::Utf8,
        }
    }
}

#[derive(Clone, Debug)]
pub enum IrcEvent {
    /// TCP/TLS handshake completed.
    SocketConnected,
    /// A raw IRC line received from the server (`inbound: true`) or sent by
    /// us as an outbound echo (`inbound: false`).
    Raw { line: String, inbound: bool },
    /// Connection closed.
    Closed,
    /// A non-fatal or fatal error. A fatal error is followed by `Closed`.
    Error(String),
}

#[derive(Clone)]
pub struct IrcClient {
    cmd_tx: mpsc::Sender<ClientCommand>,
}

#[derive(Debug)]
enum ClientCommand {
    Send(String),
    Quit(Option<String>),
    Disconnect,
}

impl IrcClient {
    /// Connect to an IRC server. Returns immediately with a handle and an
    /// event receiver; the actual connection happens on a spawned task and
    /// progress is reported as `IrcEvent`s.
    pub fn connect(options: IrcClientOptions) -> (Self, mpsc::Receiver<IrcEvent>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::channel(256);
        tokio::spawn(run(options, cmd_rx, event_tx));
        (Self { cmd_tx }, event_rx)
    }

    /// Send a raw IRC line. CR/LF is stripped to prevent line injection.
    pub async fn send(&self, line: impl Into<String>) -> Result<(), IrcError> {
        self.cmd_tx
            .send(ClientCommand::Send(line.into()))
            .await
            .map_err(|_| {
                IrcError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "client task gone",
                ))
            })
    }

    /// Send `QUIT [:message]` and close the connection.
    pub async fn quit(&self, message: Option<String>) -> Result<(), IrcError> {
        self.cmd_tx
            .send(ClientCommand::Quit(message))
            .await
            .map_err(|_| {
                IrcError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "client task gone",
                ))
            })
    }

    /// Forcefully drop the connection without sending QUIT.
    pub async fn disconnect(&self) -> Result<(), IrcError> {
        self.cmd_tx
            .send(ClientCommand::Disconnect)
            .await
            .map_err(|_| {
                IrcError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "client task gone",
                ))
            })
    }
}

// --- internals ---------------------------------------------------------------

/// Single trait alias so we can box the socket regardless of TLS.
trait IoStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> IoStream for T {}

async fn run(
    opts: IrcClientOptions,
    mut cmd_rx: mpsc::Receiver<ClientCommand>,
    event_tx: mpsc::Sender<IrcEvent>,
) {
    let mut stream: Box<dyn IoStream> = match connect_socket(&opts).await {
        Ok(s) => s,
        Err(e) => {
            let _ = event_tx.send(IrcEvent::Error(e.to_string())).await;
            let _ = event_tx.send(IrcEvent::Closed).await;
            return;
        }
    };

    let _ = event_tx.send(IrcEvent::SocketConnected).await;

    let mut buf = vec![0u8; 8192];
    let mut line_buffer = LineBuffer::new();
    // Caller-originated sends only. Matches the `./network` bridge, which
    // rate-limits inbound WS messages.
    let mut send_limiter = SlidingWindowLimiter::default_irc();

    loop {
        tokio::select! {
            // Socket read -> surface each line as an inbound Raw event.
            res = stream.read(&mut buf) => {
                match res {
                    Ok(0) => break,
                    Ok(n) => {
                        line_buffer.extend(&buf[..n]);
                        if line_buffer.len() > MAX_RECEIVE_BUFFER {
                            let _ = event_tx
                                .send(IrcEvent::Error(IrcError::BufferOverflow.to_string()))
                                .await;
                            break;
                        }
                        while let Some(line) = line_buffer.next_line(opts.encoding) {
                            let _ = event_tx
                                .send(IrcEvent::Raw { line, inbound: true })
                                .await;
                        }
                    }
                    Err(e) => {
                        let _ = event_tx.send(IrcEvent::Error(e.to_string())).await;
                        break;
                    }
                }
            }

            // Outbound commands.
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ClientCommand::Send(line)) => {
                        // Over the sliding window (50 msgs / 5 s): drop
                        // silently, matching the `./network` bridge. Queuing
                        // would risk unbounded memory under a flood.
                        if !send_limiter.check_and_consume() {
                            continue;
                        }
                        if write_and_emit(&mut stream, &line, &event_tx).await.is_err() {
                            break;
                        }
                    }
                    Some(ClientCommand::Quit(msg)) => {
                        let q = match msg {
                            Some(m) => format!("QUIT :{}", strip_crlf(&m)),
                            None => "QUIT".to_string(),
                        };
                        let _ = write_and_emit(&mut stream, &q, &event_tx).await;
                        let _ = stream.shutdown().await;
                        break;
                    }
                    Some(ClientCommand::Disconnect) | None => break,
                }
            }
        }
    }

    close(&event_tx).await;
}

async fn close(event_tx: &mpsc::Sender<IrcEvent>) {
    let _ = event_tx.send(IrcEvent::Closed).await;
}

async fn write_and_emit<S: AsyncWrite + Unpin>(
    stream: &mut S,
    line: &str,
    event_tx: &mpsc::Sender<IrcEvent>,
) -> Result<(), std::io::Error> {
    let stripped = strip_crlf(line);
    let mut bytes = stripped.into_bytes();
    bytes.extend_from_slice(b"\r\n");
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    let _ = event_tx
        .send(IrcEvent::Raw {
            line: line.to_string(),
            inbound: false,
        })
        .await;
    Ok(())
}

async fn connect_socket(opts: &IrcClientOptions) -> Result<Box<dyn IoStream>, IrcError> {
    let addr = format!("{}:{}", opts.host, opts.port);
    let tcp = time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr))
        .await
        .map_err(|_| IrcError::ConnectTimeout)??;
    if !opts.tls {
        return Ok(Box::new(tcp));
    }

    let mut roots = RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        let _ = roots.add(cert);
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| IrcError::Tls(e.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();

    let connector = TlsConnector::from(Arc::new(config));
    let server_name = ServerName::try_from(opts.host.clone())
        .map_err(|e| IrcError::InvalidHostname(format!("{}: {}", opts.host, e)))?;

    let tls = time::timeout(CONNECT_TIMEOUT, connector.connect(server_name, tcp))
        .await
        .map_err(|_| IrcError::ConnectTimeout)?
        .map_err(|e| IrcError::Tls(e.to_string()))?;

    Ok(Box::new(tls))
}
