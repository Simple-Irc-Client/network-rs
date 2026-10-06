//! DCC CHAT: newline-framed text over the DCC socket.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::codec::{strip_crlf, Encoding, LineBuffer, MAX_RECEIVE_BUFFER};

use super::stream::DccStream;
use super::{ChatCommand, DccError, DccEvent};

/// Runs until the peer hangs up or the caller closes the session.
pub async fn run_chat(
    mut stream: DccStream,
    commands: &mut mpsc::Receiver<ChatCommand>,
    events: &mpsc::Sender<DccEvent>,
    encoding: Encoding,
) -> Result<(), DccError> {
    let mut read_buf = vec![0u8; 8192];
    let mut lines = LineBuffer::default();

    loop {
        tokio::select! {
            read = stream.io.read(&mut read_buf) => {
                let n = read?;
                if n == 0 {
                    return Ok(());
                }
                lines.extend(&read_buf[..n]);
                while let Some(text) = lines.next_line(encoding) {
                    let _ = events.send(DccEvent::Line { text }).await;
                }
                if lines.len() > MAX_RECEIVE_BUFFER {
                    return Err(DccError::BufferOverflow);
                }
            }

            command = commands.recv() => match command {
                Some(ChatCommand::SendLine(text)) => {
                    let mut bytes = strip_crlf(&text).into_bytes();
                    bytes.push(b'\n');
                    stream.io.write_all(&bytes).await?;
                    stream.io.flush().await?;
                }
                Some(ChatCommand::Close) | None => return Ok(()),
            },
        }
    }
}
