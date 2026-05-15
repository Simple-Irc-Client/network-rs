//! Async IRC client.
//!
//! The driver runs as a single tokio task that owns the socket and a `select!`
//! loop over:
//!   - socket reads (line buffered)
//!   - inbound commands from the caller (send / quit / disconnect)
//!   - the PING keepalive interval
//!   - the PONG response deadline (active only while waiting for one)
//!   - the CAP LS response deadline (active only during capability negotiation)
//!
//! Events flow out over an `mpsc::Receiver<IrcEvent>` so the consumer can
//! forward them however it likes.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use crate::codec::{strip_crlf, LineBuffer, MAX_RECEIVE_BUFFER};
use crate::error::IrcError;
use crate::ratelimit::SlidingWindowLimiter;

const PING_INTERVAL: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_PONG_TIMEOUT: Duration = Duration::from_secs(120);
const CAP_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const CAP_MAX_RETRIES: u32 = 2;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Encoding {
    #[default]
    Utf8,
    Latin1,
}

#[derive(Clone, Debug)]
pub struct RegistrationOptions {
    pub nick: String,
    pub username: String,
    pub gecos: String,
}

#[derive(Clone, Debug)]
pub struct IrcClientOptions {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub encoding: Encoding,
    pub pong_timeout: Duration,
    /// `Some` -> "full" mode: client sends `CAP LS 302`, `NICK`, `USER` after
    /// the socket is up. `None` -> "raw" mode: caller drives registration.
    pub registration: Option<RegistrationOptions>,
}

impl IrcClientOptions {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            tls: false,
            encoding: Encoding::Utf8,
            pong_timeout: DEFAULT_PONG_TIMEOUT,
            registration: None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum IrcEvent {
    /// TCP/TLS handshake completed.
    SocketConnected,
    /// Server sent RPL_WELCOME (numeric 001) — registration succeeded.
    Connected,
    /// A raw IRC line received from the server (`inbound: true`) or sent by
    /// us as an outbound echo (`inbound: false`).
    Raw { line: String, inbound: bool },
    /// CAP LS negotiation timed out after the configured retries.
    CapTimeout { retries: u32 },
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

#[derive(Debug)]
enum CapState {
    /// CAP LS not in flight (raw mode, or already finished).
    Done,
    /// Awaiting the server's CAP LS response.
    Waiting { retries: u32, deadline: Instant },
}

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

    let mut cap_state = CapState::Done;
    if let Some(reg) = &opts.registration {
        // Order: CAP LS 302, NICK, USER. CAP timer starts the moment we send CAP LS.
        if write_and_emit(&mut stream, "CAP LS 302", &event_tx)
            .await
            .is_err()
        {
            close(&event_tx).await;
            return;
        }
        cap_state = CapState::Waiting {
            retries: 0,
            deadline: Instant::now() + CAP_RESPONSE_TIMEOUT,
        };
        let nick_line = format!("NICK {}", strip_crlf(&reg.nick));
        if write_and_emit(&mut stream, &nick_line, &event_tx)
            .await
            .is_err()
        {
            close(&event_tx).await;
            return;
        }
        let user_line = format!(
            "USER {} 0 * :{}",
            strip_crlf(&reg.username),
            strip_crlf(&reg.gecos),
        );
        if write_and_emit(&mut stream, &user_line, &event_tx)
            .await
            .is_err()
        {
            close(&event_tx).await;
            return;
        }
    }

    let mut ping_interval = time::interval(PING_INTERVAL);
    // The first tick of `interval()` fires immediately — skip it so we don't
    // PING right after the socket comes up.
    ping_interval.tick().await;

    let mut pong_deadline: Option<Instant> = None;
    let mut buf = vec![0u8; 8192];
    let mut line_buffer = LineBuffer::new();
    // Caller-originated sends only. Protocol traffic the driver generates
    // itself (registration, PING/PONG, CAP) goes straight through
    // `write_and_emit` and is intentionally exempt — matching the
    // `./network` bridge, which rate-limits inbound WS messages but not the
    // IRC client's own keepalive/registration.
    let mut send_limiter = SlidingWindowLimiter::default_irc();

    loop {
        let cap_deadline = match cap_state {
            CapState::Waiting { deadline, .. } => Some(deadline),
            CapState::Done => None,
        };

        tokio::select! {
            // Socket read.
            res = stream.read(&mut buf) => {
                match res {
                    Ok(0) => break,
                    Ok(n) => {
                        line_buffer.extend(&buf[..n]);
                        // Any data from the server proves it's alive; clear PONG deadline.
                        pong_deadline = None;
                        if line_buffer.len() > MAX_RECEIVE_BUFFER {
                            let _ = event_tx
                                .send(IrcEvent::Error(IrcError::BufferOverflow.to_string()))
                                .await;
                            break;
                        }
                        while let Some(line) = line_buffer.next_line(opts.encoding) {
                            handle_incoming_line(
                                &line,
                                &event_tx,
                                &mut stream,
                                &mut cap_state,
                            )
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

            // PING keepalive.
            _ = ping_interval.tick() => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0);
                let line = format!("PING :{}", now_ms);
                if write_and_emit(&mut stream, &line, &event_tx).await.is_err() {
                    break;
                }
                pong_deadline = Some(Instant::now() + opts.pong_timeout);
            }

            // PONG deadline.
            _ = sleep_until_opt(pong_deadline) => {
                let _ = event_tx
                    .send(IrcEvent::Error(IrcError::PongTimeout.to_string()))
                    .await;
                break;
            }

            // CAP LS response deadline.
            _ = sleep_until_opt(cap_deadline) => {
                if let CapState::Waiting { retries, .. } = cap_state {
                    if retries < CAP_MAX_RETRIES {
                        let next = retries + 1;
                        if write_and_emit(&mut stream, "CAP LS 302", &event_tx).await.is_err() {
                            break;
                        }
                        cap_state = CapState::Waiting {
                            retries: next,
                            deadline: Instant::now() + CAP_RESPONSE_TIMEOUT,
                        };
                    } else {
                        let _ = event_tx
                            .send(IrcEvent::CapTimeout { retries })
                            .await;
                        cap_state = CapState::Done;
                    }
                }
            }
        }
    }

    close(&event_tx).await;
}

async fn close(event_tx: &mpsc::Sender<IrcEvent>) {
    let _ = event_tx.send(IrcEvent::Closed).await;
}

/// Sleep until `at`, or never if `at` is None.
async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(t) => time::sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
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

async fn handle_incoming_line<S: AsyncWrite + Unpin>(
    line: &str,
    event_tx: &mpsc::Sender<IrcEvent>,
    stream: &mut S,
    cap_state: &mut CapState,
) {
    let _ = event_tx
        .send(IrcEvent::Raw {
            line: line.to_string(),
            inbound: true,
        })
        .await;

    // Auto-reply to PING.
    if let Some(rest) = line.strip_prefix("PING ") {
        let pong = format!("PONG {}", rest);
        let _ = write_and_emit(stream, &pong, event_tx).await;
        return;
    }

    // CAP LS handling — both prefixed (":server CAP nick LS …") and bare
    // ("CAP * LS …") forms.
    if let Some(is_continuation) = parse_cap_ls(line) {
        let was_waiting = matches!(cap_state, CapState::Waiting { .. });
        if was_waiting && !is_continuation {
            let _ = write_and_emit(stream, "CAP END", event_tx).await;
            *cap_state = CapState::Done;
        } else if was_waiting {
            // Continuation: extend the deadline so retries don't fire while
            // the server is mid-stream.
            if let CapState::Waiting { retries, .. } = cap_state {
                *cap_state = CapState::Waiting {
                    retries: *retries,
                    deadline: Instant::now() + CAP_RESPONSE_TIMEOUT,
                };
            }
        }
        return;
    }

    // RPL_WELCOME → registered.
    if is_rpl_welcome(line) {
        let _ = event_tx.send(IrcEvent::Connected).await;
    }
}

/// Returns `Some(is_continuation)` if `line` looks like `CAP <client> LS [*] …`.
fn parse_cap_ls(line: &str) -> Option<bool> {
    let mut tokens = line.split_whitespace();
    while let Some(tok) = tokens.next() {
        if tok == "CAP" {
            // Next token is the client name (or *).
            tokens.next()?;
            if tokens.next() == Some("LS") {
                let is_continuation = tokens.next() == Some("*");
                return Some(is_continuation);
            }
            return None;
        }
    }
    None
}

/// Returns true for an RPL_WELCOME numeric.
/// Matches `(@tags )?:source 001 ` where source has no `!` or `@`.
fn is_rpl_welcome(line: &str) -> bool {
    let after_tags = if let Some(rest) = line.strip_prefix('@') {
        match rest.find(' ') {
            Some(i) => &rest[i + 1..],
            None => return false,
        }
    } else {
        line
    };
    let body = match after_tags.strip_prefix(':') {
        Some(s) => s,
        None => return false,
    };
    let space = match body.find(' ') {
        Some(i) => i,
        None => return false,
    };
    let source = &body[..space];
    if source.contains('!') || source.contains('@') {
        return false;
    }
    let after_source = &body[space + 1..];
    after_source.starts_with("001 ") || after_source == "001"
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

#[cfg(test)]
mod parser_tests {
    use super::*;

    #[test]
    fn rpl_welcome_basic() {
        assert!(is_rpl_welcome(":irc.example.com 001 nick :Welcome"));
    }

    #[test]
    fn rpl_welcome_with_tags() {
        assert!(is_rpl_welcome(
            "@time=2024-01-01T00:00:00Z :irc.example.com 001 nick :Welcome"
        ));
    }

    #[test]
    fn rpl_welcome_rejects_user_hostmask_source() {
        assert!(!is_rpl_welcome(":nick!user@host 001 nick :hi"));
    }

    #[test]
    fn rpl_welcome_rejects_other_numerics() {
        assert!(!is_rpl_welcome(":irc.example.com 002 nick :foo"));
    }

    #[test]
    fn cap_ls_no_prefix_final() {
        assert_eq!(parse_cap_ls("CAP * LS :sasl multi-prefix"), Some(false));
    }

    #[test]
    fn cap_ls_no_prefix_continuation() {
        assert_eq!(parse_cap_ls("CAP * LS * :sasl"), Some(true));
    }

    #[test]
    fn cap_ls_with_prefix() {
        assert_eq!(
            parse_cap_ls(":irc.example.com CAP nick LS :sasl"),
            Some(false)
        );
        assert_eq!(
            parse_cap_ls(":irc.example.com CAP nick LS * :sasl"),
            Some(true)
        );
    }

    #[test]
    fn cap_ack_is_not_cap_ls() {
        assert_eq!(parse_cap_ls("CAP * ACK :multi-prefix"), None);
    }
}
