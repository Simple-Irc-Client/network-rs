//! DCC CHAT: a newline-framed text conversation over the DCC socket.
//!
//! Reuses the IRC `LineBuffer`, including its 2 MiB cap — a peer that never
//! sends a terminator must not be able to grow our buffer without bound.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::client::Encoding;
use crate::codec::{strip_crlf, LineBuffer, MAX_RECEIVE_BUFFER};

use super::stream::DccStream;
use super::{DccCommand, DccError, DccEvent};

pub async fn run_chat(
    mut stream: DccStream,
    mut commands: mpsc::Receiver<DccCommand>,
    events: &mpsc::Sender<DccEvent>,
    encoding: Encoding,
) -> Result<(), DccError> {
    let mut buf = vec![0u8; 8192];
    let mut lines = LineBuffer::new();

    loop {
        tokio::select! {
            read = stream.io.read(&mut buf) => {
                match read {
                    Ok(0) => return Ok(()),
                    Ok(n) => {
                        lines.extend(&buf[..n]);
                        if lines.len() > MAX_RECEIVE_BUFFER {
                            return Err(DccError::BufferOverflow);
                        }
                        while let Some(text) = lines.next_line(encoding) {
                            let _ = events.send(DccEvent::Line { text }).await;
                        }
                    }
                    Err(e) => return Err(DccError::Io(e)),
                }
            }

            command = commands.recv() => {
                match command {
                    Some(DccCommand::SendLine(text)) => {
                        let mut bytes = strip_crlf(&text).into_bytes();
                        bytes.extend_from_slice(b"\n");
                        stream.io.write_all(&bytes).await?;
                        stream.io.flush().await?;
                    }
                    Some(DccCommand::Close) | None => return Ok(()),
                }
            }
        }
    }
}
