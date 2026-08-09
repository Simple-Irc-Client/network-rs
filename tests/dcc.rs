//! Integration tests for the DCC transport against an in-process mock peer.
//!
//! No real network: every test binds `127.0.0.1:0` on one side and points the
//! session at it. Both roles (offerer/listener and acceptor/dialler) and both
//! kinds (chat and file transfer) are exercised, plus the failure paths that
//! matter — a peer that vanishes mid-transfer, one that sends more than it
//! announced, and one connecting from the wrong address.

use std::io::Write;
use std::time::Duration;

use sic_irc::dcc::{DccConnectOptions, DccError, DccEvent, DccListenOptions, DccSession};
use sic_irc::Encoding;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(10);

async fn bind_local() -> (u16, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (port, listener)
}

/// Pull events until one matches, failing fast rather than hanging the suite.
async fn wait_for(
    rx: &mut mpsc::Receiver<DccEvent>,
    pred: impl Fn(&DccEvent) -> bool,
    label: &str,
) -> DccEvent {
    let found = timeout(WAIT, async {
        while let Some(event) = rx.recv().await {
            if pred(&event) {
                return Some(event);
            }
        }
        None
    })
    .await;

    match found {
        Ok(Some(event)) => event,
        Ok(None) => panic!("channel closed before {label}"),
        Err(_) => panic!("timed out waiting for {label}"),
    }
}

fn connect_options(port: u16) -> DccConnectOptions {
    DccConnectOptions {
        host: "127.0.0.1".to_string(),
        port,
        secure: false,
        save_path: None,
        size: None,
        encoding: Encoding::Utf8,
    }
}

fn listen_options() -> DccListenOptions {
    DccListenOptions {
        secure: false,
        port_start: 0,
        port_end: 0,
        expect_peer: None,
        file_path: None,
        encoding: Encoding::Utf8,
    }
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("sic-dcc-test-{}-{}", std::process::id(), name));
    path
}

// --- chat --------------------------------------------------------------------

#[tokio::test]
async fn chat_receives_lines_from_the_peer() {
    let (port, listener) = bind_local().await;

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"hello\nworld\r\n").await.unwrap();
        // Hold the socket open so the reader does not see EOF mid-assertion.
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let (_session, mut events) = DccSession::connect(connect_options(port));

    let first = wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Line { .. }),
        "first line",
    )
    .await;
    assert!(matches!(first, DccEvent::Line { text } if text == "hello"));

    let second = wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Line { .. }),
        "second line",
    )
    .await;
    // A bare \n and a \r\n must both frame, since real clients send either.
    assert!(matches!(second, DccEvent::Line { text } if text == "world"));
}

#[tokio::test]
async fn chat_sends_newline_terminated_lines() {
    let (port, listener) = bind_local().await;

    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 64];
        let n = socket.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    });

    let (session, mut events) = DccSession::connect(connect_options(port));
    wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Connected { .. }),
        "connect",
    )
    .await;
    session.send_line("hi there").await.unwrap();

    assert_eq!(peer.await.unwrap(), "hi there\n");
}

#[tokio::test]
async fn chat_strips_embedded_newlines_from_outgoing_text() {
    let (port, listener) = bind_local().await;

    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 64];
        let n = socket.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    });

    let (session, mut events) = DccSession::connect(connect_options(port));
    wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Connected { .. }),
        "connect",
    )
    .await;
    // A peer must never be able to inject an extra frame through our sender.
    session.send_line("a\r\nb").await.unwrap();

    assert_eq!(peer.await.unwrap(), "ab\n");
}

#[tokio::test]
async fn chat_closes_when_the_peer_hangs_up() {
    let (port, listener) = bind_local().await;

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        drop(socket);
    });

    let (_session, mut events) = DccSession::connect(connect_options(port));
    wait_for(&mut events, |e| matches!(e, DccEvent::Closed), "close").await;
}

// --- listening side ----------------------------------------------------------

#[tokio::test]
async fn listen_reports_its_port_then_accepts_a_chat() {
    let (session, port, mut events) = DccSession::listen(listen_options()).unwrap();
    assert!(port > 0);

    let listening = wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Listening { .. }),
        "listening",
    )
    .await;
    assert!(matches!(listening, DccEvent::Listening { port: p } if p == port));

    let peer = tokio::spawn(async move {
        let mut socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        socket.write_all(b"from the peer\n").await.unwrap();
        let mut buf = vec![0u8; 64];
        let n = socket.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    });

    wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Connected { .. }),
        "connected",
    )
    .await;
    let line = wait_for(&mut events, |e| matches!(e, DccEvent::Line { .. }), "line").await;
    assert!(matches!(line, DccEvent::Line { text } if text == "from the peer"));

    session.send_line("and back").await.unwrap();
    assert_eq!(peer.await.unwrap(), "and back\n");
}

#[tokio::test]
async fn listener_ignores_a_connection_from_the_wrong_address() {
    // Expect a peer that is not loopback, then connect from loopback: the
    // session must keep waiting rather than talk to the wrong host.
    let options = DccListenOptions {
        expect_peer: Some("203.0.113.1".parse().unwrap()),
        ..listen_options()
    };
    let (_session, port, mut events) = DccSession::listen(options).unwrap();
    wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Listening { .. }),
        "listening",
    )
    .await;

    let mut socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let _ = socket.write_all(b"intruder\n").await;

    let saw_connect = timeout(Duration::from_millis(400), async {
        while let Some(event) = events.recv().await {
            if matches!(event, DccEvent::Connected { .. } | DccEvent::Line { .. }) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);

    assert!(
        !saw_connect,
        "session accepted a connection from an unexpected address"
    );
}

// --- file transfer -----------------------------------------------------------

#[tokio::test]
async fn receives_a_file_and_acks_every_chunk() {
    let payload = vec![7u8; 200 * 1024];
    let (port, listener) = bind_local().await;

    let sent = payload.clone();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(&sent).await.unwrap();
        socket.flush().await.unwrap();
        // Read the running-total acks the receiver owes us.
        let mut acks = Vec::new();
        let mut buf = [0u8; 4];
        while acks.len() < 4 {
            match socket.read_exact(&mut buf).await {
                Ok(_) => acks.push(u32::from_be_bytes(buf)),
                Err(_) => break,
            }
        }
        acks
    });

    let path = temp_path("recv.bin");
    let options = DccConnectOptions {
        save_path: Some(path.clone()),
        size: Some(payload.len() as u64),
        ..connect_options(port)
    };
    let (_session, mut events) = DccSession::connect(options);

    let completed = wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Completed { .. }),
        "completed",
    )
    .await;
    assert!(matches!(completed, DccEvent::Completed { path: Some(_) }));

    let acks = peer.await.unwrap();
    assert!(!acks.is_empty(), "receiver never acknowledged any data");
    assert!(
        acks.windows(2).all(|w| w[0] <= w[1]),
        "acks must be monotonic"
    );

    let written = std::fs::read(&path).unwrap();
    assert_eq!(written, payload);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn fails_and_removes_the_file_when_the_peer_sends_too_little() {
    let (port, listener) = bind_local().await;

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(&[1u8; 100]).await.unwrap();
        socket.flush().await.unwrap();
        drop(socket);
    });

    let path = temp_path("short.bin");
    let options = DccConnectOptions {
        save_path: Some(path.clone()),
        size: Some(4096),
        ..connect_options(port)
    };
    let (_session, mut events) = DccSession::connect(options);

    let error = wait_for(&mut events, |e| matches!(e, DccEvent::Error(_)), "error").await;
    assert!(matches!(error, DccEvent::Error(msg) if msg.contains("size mismatch")));
    // A truncated download must not be left behind looking complete.
    assert!(!path.exists());
}

#[tokio::test]
async fn fails_when_the_peer_sends_more_than_it_announced() {
    let (port, listener) = bind_local().await;

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = socket.write_all(&vec![2u8; 128 * 1024]).await;
        let _ = socket.flush().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let path = temp_path("long.bin");
    let options = DccConnectOptions {
        save_path: Some(path.clone()),
        size: Some(1024),
        ..connect_options(port)
    };
    let (_session, mut events) = DccSession::connect(options);

    let error = wait_for(&mut events, |e| matches!(e, DccEvent::Error(_)), "error").await;
    assert!(matches!(error, DccEvent::Error(msg) if msg.contains("size mismatch")));
    assert!(!path.exists());
}

#[tokio::test]
async fn sends_a_file_to_a_peer_that_connects() {
    let path = temp_path("send.bin");
    let payload = vec![9u8; 300 * 1024];
    std::fs::File::create(&path)
        .unwrap()
        .write_all(&payload)
        .unwrap();

    let options = DccListenOptions {
        file_path: Some(path.clone()),
        ..listen_options()
    };
    let (_session, port, mut events) = DccSession::listen(options).unwrap();
    wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Listening { .. }),
        "listening",
    )
    .await;

    let expected_len = payload.len();
    let peer = tokio::spawn(async move {
        let mut socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut received = Vec::with_capacity(expected_len);
        let mut buf = vec![0u8; 32 * 1024];
        while received.len() < expected_len {
            let n = socket.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            received.extend_from_slice(&buf[..n]);
            // Ack the running total, as a real DCC receiver does.
            let _ = socket
                .write_all(&(received.len() as u32).to_be_bytes())
                .await;
        }
        received
    });

    wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Completed { .. }),
        "completed",
    )
    .await;
    assert_eq!(peer.await.unwrap(), payload);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn reports_progress_while_transferring() {
    let payload = vec![3u8; 900 * 1024];
    let (port, listener) = bind_local().await;

    let sent = payload.clone();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = socket.write_all(&sent).await;
        let _ = socket.flush().await;
        // Keep draining acks so the sender side never blocks.
        let mut sink = [0u8; 256];
        while socket.read(&mut sink).await.unwrap_or(0) > 0 {}
    });

    let path = temp_path("progress.bin");
    let options = DccConnectOptions {
        save_path: Some(path.clone()),
        size: Some(payload.len() as u64),
        ..connect_options(port)
    };
    let (_session, mut events) = DccSession::connect(options);

    let progress = wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Progress { .. }),
        "progress",
    )
    .await;
    assert!(matches!(progress, DccEvent::Progress { transferred } if transferred > 0));

    wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Completed { .. }),
        "completed",
    )
    .await;
    let _ = std::fs::remove_file(&path);
}

// --- secure DCC --------------------------------------------------------------

#[tokio::test]
async fn secure_chat_completes_a_tls_handshake_and_reports_a_fingerprint() {
    let (offerer, port, mut offerer_events) = DccSession::listen(DccListenOptions {
        secure: true,
        ..listen_options()
    })
    .unwrap();
    wait_for(
        &mut offerer_events,
        |e| matches!(e, DccEvent::Listening { .. }),
        "listening",
    )
    .await;

    let (acceptor, mut acceptor_events) = DccSession::connect(DccConnectOptions {
        secure: true,
        ..connect_options(port)
    });

    // The dialling side sees the peer certificate and must surface its
    // fingerprint — it is the only thing a user can check for secure DCC.
    let connected = wait_for(
        &mut acceptor_events,
        |e| matches!(e, DccEvent::Connected { .. }),
        "acceptor connected",
    )
    .await;
    match connected {
        DccEvent::Connected { tls_fingerprint } => {
            let fingerprint = tls_fingerprint.expect("secure session reported no fingerprint");
            assert_eq!(fingerprint.len(), 32 * 3 - 1);
            assert!(fingerprint.contains(':'));
        }
        other => panic!("unexpected event: {other:?}"),
    }

    wait_for(
        &mut offerer_events,
        |e| matches!(e, DccEvent::Connected { .. }),
        "offerer connected",
    )
    .await;

    // And the encrypted channel actually carries text both ways.
    acceptor.send_line("secure hello").await.unwrap();
    let line = wait_for(
        &mut offerer_events,
        |e| matches!(e, DccEvent::Line { .. }),
        "line",
    )
    .await;
    assert!(matches!(line, DccEvent::Line { text } if text == "secure hello"));

    offerer.send_line("secure reply").await.unwrap();
    let reply = wait_for(
        &mut acceptor_events,
        |e| matches!(e, DccEvent::Line { .. }),
        "reply",
    )
    .await;
    assert!(matches!(reply, DccEvent::Line { text } if text == "secure reply"));
}

#[tokio::test]
async fn secure_transfer_moves_a_file_over_tls() {
    let path = temp_path("secure-send.bin");
    let payload = vec![5u8; 128 * 1024];
    std::fs::File::create(&path)
        .unwrap()
        .write_all(&payload)
        .unwrap();

    let (_offerer, port, mut offerer_events) = DccSession::listen(DccListenOptions {
        secure: true,
        file_path: Some(path.clone()),
        ..listen_options()
    })
    .unwrap();
    wait_for(
        &mut offerer_events,
        |e| matches!(e, DccEvent::Listening { .. }),
        "listening",
    )
    .await;

    let save_path = temp_path("secure-recv.bin");
    let (_acceptor, mut acceptor_events) = DccSession::connect(DccConnectOptions {
        secure: true,
        save_path: Some(save_path.clone()),
        size: Some(payload.len() as u64),
        ..connect_options(port)
    });

    wait_for(
        &mut acceptor_events,
        |e| matches!(e, DccEvent::Completed { .. }),
        "completed",
    )
    .await;

    assert_eq!(std::fs::read(&save_path).unwrap(), payload);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&save_path);
}

#[tokio::test]
async fn a_plain_peer_cannot_speak_to_a_secure_session() {
    let (_session, port, mut events) = DccSession::listen(DccListenOptions {
        secure: true,
        ..listen_options()
    })
    .unwrap();
    wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Listening { .. }),
        "listening",
    )
    .await;

    tokio::spawn(async move {
        let mut socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        // Cleartext where a ClientHello is expected.
        let _ = socket.write_all(b"not tls at all\n").await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let error = wait_for(
        &mut events,
        |e| matches!(e, DccEvent::Error(_)),
        "tls error",
    )
    .await;
    assert!(matches!(error, DccEvent::Error(msg) if msg.contains("TLS")));
}

// --- errors ------------------------------------------------------------------

#[tokio::test]
async fn connecting_to_a_closed_port_reports_an_error_then_closes() {
    let (port, listener) = bind_local().await;
    drop(listener);

    let (_session, mut events) = DccSession::connect(connect_options(port));

    wait_for(&mut events, |e| matches!(e, DccEvent::Error(_)), "error").await;
    wait_for(&mut events, |e| matches!(e, DccEvent::Closed), "close").await;
}

#[tokio::test]
async fn size_mismatch_error_names_both_counts() {
    let error = DccError::SizeMismatch {
        expected: 10,
        actual: 4,
    };
    assert_eq!(
        error.to_string(),
        "transfer size mismatch: expected 10 bytes, got 4"
    );
}
