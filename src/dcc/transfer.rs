//! DCC SEND/GET byte pumps.
//!
//! The classic protocol: the sender streams the file; the receiver replies with
//! a 4-byte big-endian running total after each chunk. Modern clients treat
//! those acks as advisory, but plenty of old ones stall without them, so the
//! receiver always sends them and the sender reads them to bound how far ahead
//! it may run.
//!
//! The counter is 32-bit and wraps past 4 GiB. That is the protocol, not a bug
//! here — so acks are used only for flow control, never as the completion
//! signal. Completion is decided by the byte count we actually moved.

use std::path::Path;
use std::time::Duration;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time;

use super::stream::DccStream;
use super::{DccError, DccEvent};

const CHUNK: usize = 64 * 1024;

/// How far the sender may run ahead of the last acknowledged byte. Bounded so a
/// receiver that stops acking cannot make us buffer the whole file in flight.
const SEND_AHEAD_WINDOW: u64 = 1024 * 1024;

/// Emit progress at most this often, so a fast local transfer doesn't flood the
/// event channel with one message per 64 KiB.
const PROGRESS_INTERVAL: u64 = 256 * 1024;

/// How long the sender waits for the receiver's closing ack before giving up on
/// it. The bytes are already delivered at this point, so timing out here still
/// counts as a completed transfer.
const FINAL_ACK_TIMEOUT: Duration = Duration::from_secs(30);

/// Stream `path` to the peer, reading acks to stay inside the send-ahead window.
pub async fn send_file(
    mut stream: DccStream,
    path: &Path,
    events: &mpsc::Sender<DccEvent>,
) -> Result<(), DccError> {
    let mut file = File::open(path).await?;
    let total = file.metadata().await?.len();

    let mut buf = vec![0u8; CHUNK];
    let mut ack_buf = [0u8; 4];
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

        if sent - last_reported >= PROGRESS_INTERVAL || sent == total {
            last_reported = sent;
            let _ = events.send(DccEvent::Progress { transferred: sent }).await;
        }

        // Drain acks until we are back inside the window. A peer that never
        // acks will block here, and the session timeout upstream ends it.
        while sent.saturating_sub(acked) > SEND_AHEAD_WINDOW {
            stream.io.read_exact(&mut ack_buf).await?;
            acked = acked.max(u32::from_be_bytes(ack_buf) as u64);
        }
    }

    stream.io.flush().await?;

    if sent != total {
        return Err(DccError::SizeMismatch {
            expected: total,
            actual: sent,
        });
    }

    // Wait for the receiver's final ack before closing. Two reasons: the
    // protocol says the sender does, and dropping a socket that still has
    // unread acks queued makes the kernel send RST, which the receiver sees as
    // a reset connection instead of a clean end.
    //
    // Past 4 GiB the 32-bit counter wraps and a final ack is not identifiable,
    // so there is nothing to wait for.
    if total <= u64::from(u32::MAX) {
        let target = total as u32;
        let _ = time::timeout(FINAL_ACK_TIMEOUT, async {
            while acked < total {
                // EOF here means the peer closed without a last ack, which
                // plenty of clients do — that is a completed transfer, not a
                // failure.
                if stream.io.read_exact(&mut ack_buf).await.is_err() {
                    return;
                }
                let value = u32::from_be_bytes(ack_buf);
                acked = acked.max(u64::from(value));
                if value == target {
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

/// Receive `expected` bytes into `path`, acking as we go.
///
/// A transfer that ends early, or one that tries to write more than announced,
/// fails loudly. Silently keeping a truncated file is worse than no file: the
/// user would find a plausible-looking download that is quietly corrupt.
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

        if let Some(total) = expected {
            if received > total {
                // Refuse to keep writing past the announced size — that is how
                // a "small" offer turns into a disk-filling one.
                let _ = file.flush().await;
                let _ = tokio::fs::remove_file(path).await;
                return Err(DccError::SizeMismatch {
                    expected: total,
                    actual: received,
                });
            }
        }

        file.write_all(&buf[..read]).await?;

        // Wrapping is intended: the ack field is 32-bit by definition.
        let ack = (received as u32).to_be_bytes();
        stream.io.write_all(&ack).await?;
        stream.io.flush().await?;

        if received - last_reported >= PROGRESS_INTERVAL {
            last_reported = received;
            let _ = events
                .send(DccEvent::Progress {
                    transferred: received,
                })
                .await;
        }

        // Stop at the announced size rather than waiting for EOF. A DCC sender
        // typically holds the socket open until it sees the final ack, so
        // reading until EOF would deadlock: it waits for our ack, we wait for
        // its close.
        if expected == Some(received) {
            break;
        }
    }

    file.flush().await?;
    file.sync_all().await?;

    if let Some(total) = expected {
        if received != total {
            let _ = tokio::fs::remove_file(path).await;
            return Err(DccError::SizeMismatch {
                expected: total,
                actual: received,
            });
        }
    }

    let _ = events
        .send(DccEvent::Progress {
            transferred: received,
        })
        .await;
    Ok(())
}
