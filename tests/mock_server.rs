//! Integration tests against an in-process mock IRC server.
//!
//! No real network: each test spins up a `TcpListener` on `127.0.0.1:0`, hands
//! the assigned port to the client, and scripts both sides of the conversation.

use std::time::Duration;

use sic_irc::{IrcClient, IrcClientOptions, IrcEvent, RegistrationOptions};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;

/// Listen on a random port on loopback and return (port, listener).
async fn bind_local() -> (u16, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (port, listener)
}

/// Pull events until one matches the predicate, with a timeout. Returns the
/// matched event so the caller can assert on its contents. Panics on timeout
/// so a hung test fails fast instead of running out the test framework's
/// 60-second outer timeout.
async fn wait_for(
    rx: &mut mpsc::Receiver<IrcEvent>,
    pred: impl Fn(&IrcEvent) -> bool,
    label: &str,
) -> IrcEvent {
    // CI runners are slow when shoveling MiBs across loopback under a
    // current-thread runtime. 30 s is plenty for any non-buggy test path.
    let deadline = Duration::from_secs(30);
    timeout(deadline, async {
        loop {
            let ev = rx.recv().await.expect("event channel closed");
            if pred(&ev) {
                return ev;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timeout waiting for {label}"))
}

/// Read one CRLF-terminated line from the server-side socket.
async fn read_line<R: AsyncBufRead + Unpin>(reader: &mut R) -> String {
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    while buf.ends_with('\n') || buf.ends_with('\r') {
        buf.pop();
    }
    buf
}

#[tokio::test]
async fn raw_mode_connects_and_emits_socket_connected() {
    let (port, listener) = bind_local().await;

    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        // Hold the connection open briefly so the client side can observe it.
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(sock);
    });

    let mut opts = IrcClientOptions::new("127.0.0.1", port);
    opts.pong_timeout = Duration::from_secs(1);
    let (_client, mut events) = IrcClient::connect(opts);

    let ev = wait_for(
        &mut events,
        |e| matches!(e, IrcEvent::SocketConnected),
        "SocketConnected",
    )
    .await;
    assert!(matches!(ev, IrcEvent::SocketConnected));

    server.await.unwrap();
}

#[tokio::test]
async fn full_mode_sends_cap_nick_user_and_handles_welcome() {
    let (port, listener) = bind_local().await;

    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (rd, mut wr) = sock.into_split();
        let mut reader = BufReader::new(rd);

        let cap = read_line(&mut reader).await;
        assert_eq!(cap, "CAP LS 302");
        let nick = read_line(&mut reader).await;
        assert_eq!(nick, "NICK testnick");
        let user = read_line(&mut reader).await;
        assert_eq!(user, "USER testuser 0 * :Test User");

        // Respond with CAP LS final, expect CAP END, then send 001.
        wr.write_all(b":mock CAP * LS :sasl multi-prefix\r\n")
            .await
            .unwrap();
        let cap_end = read_line(&mut reader).await;
        assert_eq!(cap_end, "CAP END");

        wr.write_all(b":mock 001 testnick :Welcome\r\n")
            .await
            .unwrap();
        // Keep the socket open a moment so the client surface the events.
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut opts = IrcClientOptions::new("127.0.0.1", port);
    opts.registration = Some(RegistrationOptions {
        nick: "testnick".into(),
        username: "testuser".into(),
        gecos: "Test User".into(),
        negotiate_caps: true,
    });
    opts.pong_timeout = Duration::from_secs(5);

    let (_client, mut events) = IrcClient::connect(opts);

    wait_for(
        &mut events,
        |e| matches!(e, IrcEvent::SocketConnected),
        "SocketConnected",
    )
    .await;
    wait_for(
        &mut events,
        |e| matches!(e, IrcEvent::Connected),
        "Connected",
    )
    .await;

    server.await.unwrap();
}

#[tokio::test]
async fn transport_mode_opens_cap_but_never_sends_cap_end() {
    // With `negotiate_caps: false` the driver must open negotiation (CAP LS /
    // NICK / USER) so a higher layer can take over, but must NOT manage CAP
    // itself: no CAP END in response to the server's listing, no LS retries.
    let (port, listener) = bind_local().await;

    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (rd, mut wr) = sock.into_split();
        let mut reader = BufReader::new(rd);

        assert_eq!(read_line(&mut reader).await, "CAP LS 302");
        assert_eq!(read_line(&mut reader).await, "NICK testnick");
        assert_eq!(read_line(&mut reader).await, "USER testuser 0 * :Test User");

        // Send the final CAP LS listing. A standalone client would now fire
        // CAP END; this driver must stay silent.
        wr.write_all(b":mock CAP * LS :sasl multi-prefix\r\n")
            .await
            .unwrap();

        // Nothing more should arrive on its own. (CAP_RESPONSE_TIMEOUT is 10s,
        // so 2s with no auto CAP END / no retry is a sound assertion.)
        let mut buf = String::new();
        let idle = timeout(Duration::from_secs(2), reader.read_line(&mut buf)).await;
        assert!(
            idle.is_err(),
            "driver must not send anything (got {buf:?}) when negotiate_caps is false"
        );

        // The higher layer would eventually CAP END; emulate the server then
        // completing registration so we can prove the read loop is still live.
        wr.write_all(b":mock 001 testnick :Welcome\r\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut opts = IrcClientOptions::new("127.0.0.1", port);
    opts.registration = Some(RegistrationOptions {
        nick: "testnick".into(),
        username: "testuser".into(),
        gecos: "Test User".into(),
        negotiate_caps: false,
    });
    opts.pong_timeout = Duration::from_secs(5);

    let (_client, mut events) = IrcClient::connect(opts);

    wait_for(
        &mut events,
        |e| matches!(e, IrcEvent::Connected),
        "Connected",
    )
    .await;

    server.await.unwrap();
}

#[tokio::test]
async fn auto_replies_to_ping() {
    let (port, listener) = bind_local().await;

    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (rd, mut wr) = sock.into_split();
        let mut reader = BufReader::new(rd);

        wr.write_all(b"PING :payload-42\r\n").await.unwrap();
        let pong = read_line(&mut reader).await;
        assert_eq!(pong, "PONG :payload-42");
    });

    let opts = IrcClientOptions::new("127.0.0.1", port);
    let (_client, mut events) = IrcClient::connect(opts);

    wait_for(
        &mut events,
        |e| matches!(e, IrcEvent::SocketConnected),
        "SocketConnected",
    )
    .await;
    wait_for(
        &mut events,
        |e| matches!(e, IrcEvent::Raw { line, inbound: false } if line == "PONG :payload-42"),
        "PONG raw",
    )
    .await;

    server.await.unwrap();
}

#[tokio::test]
async fn closed_event_emitted_on_server_disconnect() {
    let (port, listener) = bind_local().await;

    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        // Immediate close.
        drop(sock);
    });

    let opts = IrcClientOptions::new("127.0.0.1", port);
    let (_client, mut events) = IrcClient::connect(opts);

    wait_for(&mut events, |e| matches!(e, IrcEvent::Closed), "Closed").await;

    server.await.unwrap();
}

#[tokio::test]
async fn buffer_overflow_drops_connection() {
    use sic_irc::IrcError;
    let (port, listener) = bind_local().await;

    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (_rd, mut wr) = sock.into_split();
        // Send > 2 MiB of data with no CRLF.
        let chunk = vec![b'X'; 65_536];
        for _ in 0..40 {
            if wr.write_all(&chunk).await.is_err() {
                return;
            }
        }
    });

    let opts = IrcClientOptions::new("127.0.0.1", port);
    let (_client, mut events) = IrcClient::connect(opts);

    let expected = IrcError::BufferOverflow.to_string();
    wait_for(
        &mut events,
        |e| matches!(e, IrcEvent::Error(msg) if msg == &expected),
        "BufferOverflow error",
    )
    .await;

    server.await.unwrap();
}

#[tokio::test]
async fn outbound_rate_limiter_drops_excess() {
    let (port, listener) = bind_local().await;

    let (count_tx, count_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (rd, _wr) = sock.into_split();
        let mut reader = BufReader::new(rd);
        let mut count = 0usize;
        // Count lines until the client goes idle (the dropped sends never
        // arrive, so the read stalls and the idle timeout ends the loop).
        loop {
            let mut buf = String::new();
            match timeout(Duration::from_millis(500), reader.read_line(&mut buf)).await {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                Ok(Ok(_)) => count += 1,
            }
        }
        let _ = count_tx.send(count);
    });

    // Raw mode (no registration) so the only lines the server sees are the
    // caller's sends — registration/PING/CAP are not rate-limited.
    let opts = IrcClientOptions::new("127.0.0.1", port);
    let (client, mut events) = IrcClient::connect(opts);
    wait_for(
        &mut events,
        |e| matches!(e, IrcEvent::SocketConnected),
        "SocketConnected",
    )
    .await;

    // 60 sends, well inside one 5 s window: 50 pass, 10 are dropped.
    for i in 0..60 {
        client.send(format!("PRIVMSG #c :{i}")).await.unwrap();
    }

    let count = count_rx.await.unwrap();
    assert_eq!(
        count, 50,
        "sliding-window limiter must pass exactly 50 of 60 within the window"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn send_writes_line_with_crlf_and_strips_injection() {
    let (port, listener) = bind_local().await;

    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (rd, _wr) = sock.into_split();
        let mut reader = BufReader::new(rd);
        // Injected CR/LF must collapse onto a single wire line. A single
        // read returning the whole concatenated payload IS the proof: if
        // the second segment had been sent as its own line, this read
        // would have stopped at the first \n.
        let line = read_line(&mut reader).await;
        assert_eq!(line, "PRIVMSG #c :helloINJECTED OPERevilworld");
    });

    let opts = IrcClientOptions::new("127.0.0.1", port);
    let (client, mut events) = IrcClient::connect(opts);

    wait_for(
        &mut events,
        |e| matches!(e, IrcEvent::SocketConnected),
        "SocketConnected",
    )
    .await;
    client
        .send("PRIVMSG #c :hello\r\nINJECTED OPER\nevilworld")
        .await
        .unwrap();

    server.await.unwrap();
}
