//! DCC SEND/GET.
//!
//! The sender streams the file; the receiver answers each chunk with the running byte total as a
//! 4-byte big-endian ack. The ack counter is 32-bit and wraps past 4 GiB, so acks only pace the
//! sender — completion is decided by the bytes actually moved.

use std::path::Path;
use std::time::Duration;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time;

use super::stream::DccStream;
use super::{DccError, DccEvent};

const CHUNK: usize = 64 * 1024;

/// How far the sender may run ahead of the last ack, so a receiver that stops acking can't make
/// us push the whole file into the socket.
const SEND_AHEAD_WINDOW: u64 = 1024 * 1024;

/// Progress is reported at most once per this many bytes.
const PROGRESS_INTERVAL: u64 = 256 * 1024;

/// The bytes are already delivered while waiting for the final ack, so a timeout still counts as done.
const FINAL_ACK_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn send_file(
    mut stream: DccStream,
    path: &Path,
    events: &mpsc::Sender<DccEvent>,
) -> Result<(), DccError> {
    let mut file = File::open(path).await?;
    let total = file.metadata().await?.len();

    let mut buf = vec![0u8; CHUNK];
    let mut ack = [0u8; 4];
    let mut sent: u64 = 0;
    let mut acked: u64 = 0;
    let mut last_reported: u64 = 0;

    loop {
        let read = file.read(&mut buf).await?;
        if read == 0 {
            break;
        }

        stream.io.write_all(&buf[..read]).await?;
        sent += read as u64;

        if sent - last_reported >= PROGRESS_INTERVAL {
            last_reported = sent;
            let _ = events.send(DccEvent::Progress { transferred: sent }).await;
        }

        // A peer that never acks stalls here until the caller closes the session
        while sent.saturating_sub(acked) > SEND_AHEAD_WINDOW {
            stream.io.read_exact(&mut ack).await?;
            acked = acked.max(u64::from(u32::from_be_bytes(ack)));
        }
    }

    stream.io.flush().await?;

    if sent != total {
        return Err(DccError::SizeMismatch {
            expected: total,
            actual: sent,
        });
    }

    // Closing with unread acks queued makes the kernel send RST, which the receiver sees as a reset
    // instead of a clean end. Past 4 GiB the final ack can't be recognised, so there is nothing to wait for.
    if let Ok(target) = u32::try_from(total) {
        let _ = time::timeout(FINAL_ACK_TIMEOUT, async {
            // EOF before the last ack is common and still a completed transfer
            while stream.io.read_exact(&mut ack).await.is_ok() {
                if u32::from_be_bytes(ack) == target {
                    return;
                }
            }
        })
        .await;
    }

    let _ = stream.io.shutdown().await;
    let _ = events.send(DccEvent::Progress { transferred: sent }).await;
    Ok(())
}

/// Saves the peer's file to `path`. With an announced size, a short or over-long transfer fails.
///
/// The caller removes the file when this fails.
pub async fn receive_file(
    mut stream: DccStream,
    path: &Path,
    expected: Option<u64>,
    events: &mpsc::Sender<DccEvent>,
) -> Result<(), DccError> {
    let mut file = File::create(path).await?;
    let mut buf = vec![0u8; CHUNK];
    let mut received: u64 = 0;
    let mut last_reported: u64 = 0;

    loop {
        let read = stream.io.read(&mut buf).await?;
        if read == 0 {
            break;
        }
        received += read as u64;

        // Stops a "small" offer from filling the disk
        if let Some(total) = expected.filter(|&total| received > total) {
            return Err(DccError::SizeMismatch {
                expected: total,
                actual: received,
            });
        }

        file.write_all(&buf[..read]).await?;

        // Truncation is the protocol: the ack field is 32-bit
        stream
            .io
            .write_all(&(received as u32).to_be_bytes())
            .await?;
        stream.io.flush().await?;

        if received - last_reported >= PROGRESS_INTERVAL {
            last_reported = received;
            let _ = events
                .send(DccEvent::Progress {
                    transferred: received,
                })
                .await;
        }

        // The sender usually keeps the socket open until it sees the final ack, so waiting for EOF
        // here would deadlock
        if expected == Some(received) {
            break;
        }
    }

    file.flush().await?;
    file.sync_all().await?;

    if let Some(total) = expected.filter(|&total| received != total) {
        return Err(DccError::SizeMismatch {
            expected: total,
            actual: received,
        });
    }

    let _ = events
        .send(DccEvent::Progress {
            transferred: received,
        })
        .await;
    Ok(())
}
