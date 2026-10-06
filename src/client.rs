//! IRC connection as a byte pipe.
//!
//! One tokio task owns the socket: it surfaces every received line as an [`IrcEvent::Raw`] and writes
//! the lines the caller sends. It speaks no IRC itself — registration, CAP, PING replies and
//! liveness all belong to the caller.

use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;
use tokio_rustls::TlsConnector;

use crate::codec::{strip_crlf, Encoding, LineBuffer, MAX_RECEIVE_BUFFER};
use crate::error::IrcError;
use crate::ratelimit::RateLimiter;

/// Covers the TCP connect and the TLS handshake together.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

const SEND_LIMIT_MESSAGES: u32 = 50;
const SEND_LIMIT_WINDOW: Duration = Duration::from_secs(5);

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
    SocketConnected,
    /// A line received from the server. Lines the caller sends are not echoed.
    Raw {
        line: String,
    },
    /// The connection ended. Always the last event.
    Closed,
    /// Reported just before `Closed`.
    Error(String),
}

#[derive(Debug)]
enum ClientCommand {
    Send(String),
    Quit(Option<String>),
    Disconnect,
}

/// Handle to a connection. Dropping every clone closes it.
#[derive(Clone)]
pub struct IrcClient {
    commands: mpsc::Sender<ClientCommand>,
}

impl IrcClient {
    /// Starts connecting in the background; progress arrives on the returned receiver.
    pub fn connect(options: IrcClientOptions) -> (Self, mpsc::Receiver<IrcEvent>) {
        let (command_tx, command_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::channel(256);
        tokio::spawn(run(options, command_rx, event_tx));
        (
            Self {
                commands: command_tx,
            },
            event_rx,
        )
    }

    /// Lines over the rate limit (50 per 5 s) are dropped.
    pub async fn send(&self, line: impl Into<String>) -> Result<(), IrcError> {
        self.command(ClientCommand::Send(line.into())).await
    }

    /// Sends `QUIT [:message]`, then closes the connection.
    pub async fn quit(&self, message: Option<String>) -> Result<(), IrcError> {
        self.command(ClientCommand::Quit(message)).await
    }

    /// Closes the connection without sending QUIT.
    pub async fn disconnect(&self) -> Result<(), IrcError> {
        self.command(ClientCommand::Disconnect).await
    }

    async fn command(&self, command: ClientCommand) -> Result<(), IrcError> {
        self.commands
            .send(command)
            .await
            .map_err(|_| IrcError::Closed)
    }
}

trait IoStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> IoStream for T {}

async fn run(
    options: IrcClientOptions,
    mut commands: mpsc::Receiver<ClientCommand>,
    events: mpsc::Sender<IrcEvent>,
) {
    let result = match time::timeout(CONNECT_TIMEOUT, connect_socket(&options)).await {
        Ok(Ok(stream)) => {
            let _ = events.send(IrcEvent::SocketConnected).await;
            pump(stream, options.encoding, &mut commands, &events).await
        }
        Ok(Err(error)) => Err(error),
        Err(_) => Err(IrcError::ConnectTimeout),
    };

    if let Err(error) = result {
        let _ = events.send(IrcEvent::Error(error.to_string())).await;
    }
    let _ = events.send(IrcEvent::Closed).await;
}

/// Moves lines both ways until either side closes the connection.
async fn pump(
    mut stream: Box<dyn IoStream>,
    encoding: Encoding,
    commands: &mut mpsc::Receiver<ClientCommand>,
    events: &mpsc::Sender<IrcEvent>,
) -> Result<(), IrcError> {
    let mut read_buf = vec![0u8; 8192];
    let mut lines = LineBuffer::default();
    let mut send_limiter = RateLimiter::new(SEND_LIMIT_MESSAGES, SEND_LIMIT_WINDOW);

    loop {
        tokio::select! {
            read = stream.read(&mut read_buf) => {
                let n = read?;
                if n == 0 {
                    return Ok(());
                }
                lines.extend(&read_buf[..n]);
                while let Some(line) = lines.next_line(encoding) {
                    let _ = events.send(IrcEvent::Raw { line }).await;
                }
                if lines.len() > MAX_RECEIVE_BUFFER {
                    return Err(IrcError::BufferOverflow);
                }
            }

            command = commands.recv() => match command {
                // Dropped rather than queued, so a flood can't grow memory
                Some(ClientCommand::Send(line)) => {
                    if send_limiter.try_acquire() {
                        write_line(&mut stream, &line).await?;
                    }
                }
                Some(ClientCommand::Quit(message)) => {
                    let quit = message.map_or_else(|| "QUIT".to_owned(), |text| format!("QUIT :{text}"));
                    // The connection ends either way, so a failed goodbye is not an error
                    let _ = write_line(&mut stream, &quit).await;
                    let _ = stream.shutdown().await;
                    return Ok(());
                }
                Some(ClientCommand::Disconnect) | None => return Ok(()),
            },
        }
    }
}

async fn write_line(stream: &mut (impl AsyncWrite + Unpin), line: &str) -> std::io::Result<()> {
    let mut bytes = strip_crlf(line).into_bytes();
    bytes.extend_from_slice(b"\r\n");
    stream.write_all(&bytes).await?;
    stream.flush().await
}

async fn connect_socket(options: &IrcClientOptions) -> Result<Box<dyn IoStream>, IrcError> {
    let tcp = TcpStream::connect((options.host.as_str(), options.port)).await?;
    if !options.tls {
        return Ok(Box::new(tcp));
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| IrcError::Tls(e.to_string()))?
        .with_root_certificates(load_root_store())
        .with_no_client_auth();

    let server_name = ServerName::try_from(options.host.clone())
        .map_err(|e| IrcError::InvalidHostname(format!("{}: {e}", options.host)))?;

    let tls = TlsConnector::from(Arc::new(config))
        .connect(server_name, tcp)
        .await
        .map_err(|e| IrcError::Tls(e.to_string()))?;

    Ok(Box::new(tls))
}

fn load_root_store() -> RootCertStore {
    let mut roots = RootCertStore::empty();

    #[cfg(not(target_os = "android"))]
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(cert);
    }

    // Android keeps its trust store in the Java KeyStore, which rustls-native-certs can't read
    #[cfg(target_os = "android")]
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    roots
}
