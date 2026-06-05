use std::io;

#[derive(Debug, thiserror::Error)]
pub enum IrcError {
    #[error("connection timed out")]
    ConnectTimeout,

    #[error("receive buffer overflow: server sent too much data without line terminators")]
    BufferOverflow,

    #[error("invalid hostname: {0}")]
    InvalidHostname(String),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}
