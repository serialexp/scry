//! Reconnect behaviour of [`scry_client::Client`].
//!
//! A minimal in-process mock ingest server lets us exercise the real
//! handshake/ack wire path without Garage, Valkey, or a live daemon — and,
//! crucially, lets the mock assign a **different** session id per connection so
//! we can prove the load-bearing invariant: after a reconnect the client stamps
//! the *new* server-assigned session id into every batch (a stale id would draw
//! `ERR_SESSION_MISMATCH` and a server-side disconnect).

use scry_client::Client;
use scry_proto::{
    build,
    constants::{
        ACK_ACCEPTED, CAP_AGENT_STATUS, CAP_LOGS_V2, CAP_STRUCTURED_METRICS_V2,
        PROTOCOL_VERSION_V0, PROTOCOL_VERSION_V2, SIGNAL_BIT_LOGS, SIGNAL_BIT_METRICS,
    },
    framing::{read_frame, write_frame},
    generated::FrameMsg,
    Frame,
};
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

/// Serve exactly one connection: complete the handshake announcing
/// `session_id`, then for each Batch frame report the session id it was stamped
/// with over `report` and ack it. Returns (closing the connection) after
/// `max_batches` batches or when the peer disconnects.
async fn serve_one(
    stream: TcpStream,
    session_id: u64,
    max_batches: usize,
    report: mpsc::Sender<u64>,
) {
    let (rd, wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let mut wr = BufWriter::new(wr);

    // Hello → HelloAck.
    let _hello = read_frame::<Frame, _>(&mut rd).await.expect("read hello");
    write_frame(
        &mut wr,
        &build::hello_ack(build::HelloAckArgs {
            protocol_version: PROTOCOL_VERSION_V0,
            writer_id: "test-writer",
            session_id,
            capabilities: 0,
            suggested_batch_bytes: 0,
            max_batch_bytes: 0,
            max_inflight_batches: 8,
        }),
    )
    .await
    .expect("write hello_ack");
    wr.flush().await.expect("flush hello_ack");

    let mut seen = 0;
    while seen < max_batches {
        match read_frame::<Frame, _>(&mut rd).await {
            Ok(f) => match f.msg {
                FrameMsg::Batch(b) => {
                    report.send(b.session_id).await.expect("report session id");
                    write_frame(
                        &mut wr,
                        &build::batch_ack(b.session_id, b.batch_id, ACK_ACCEPTED, 0, 0, ""),
                    )
                    .await
                    .expect("write batch_ack");
                    wr.flush().await.expect("flush batch_ack");
                    seen += 1;
                }
                FrameMsg::Goodbye(_) => return,
                _ => {}
            },
            Err(_) => return,
        }
    }
    // Drop wr/rd here → closes the socket, simulating a server restart.
}

fn test_batch(batch_id: u64) -> Frame {
    build::batch(build::BatchArgs {
        // 0 is a placeholder; send_batch_stamped overwrites it with the live
        // session id on every attempt.
        session_id: 0,
        batch_id,
        signal: 0,
        ts_min_unix_nano: 0,
        ts_max_unix_nano: 0,
        record_count: 1,
        compression: 0,
        uncompressed_size: 0,
        payload: Vec::new(),
    })
}

/// Complete one v2 handshake, report the capability request, and report the
/// session stamped onto the one batch sent over this connection.
async fn serve_combined_one(
    stream: TcpStream,
    session_id: u64,
    ack_capabilities: u32,
    hello_report: mpsc::Sender<(u16, u32)>,
    batch_report: mpsc::Sender<u64>,
) {
    let (rd, wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let mut wr = BufWriter::new(wr);

    let hello = match read_frame::<Frame, _>(&mut rd).await.unwrap().msg {
        FrameMsg::Hello(hello) => hello,
        other => panic!("expected Hello, got {other:?}"),
    };
    hello_report
        .send((hello.protocol_version, hello.capabilities))
        .await
        .unwrap();
    write_frame(
        &mut wr,
        &build::hello_ack(build::HelloAckArgs {
            protocol_version: PROTOCOL_VERSION_V2,
            writer_id: "combined-test",
            session_id,
            capabilities: ack_capabilities,
            suggested_batch_bytes: 0,
            max_batch_bytes: 0,
            max_inflight_batches: 8,
        }),
    )
    .await
    .unwrap();
    wr.flush().await.unwrap();

    match read_frame::<Frame, _>(&mut rd).await.unwrap().msg {
        FrameMsg::Batch(batch) => batch_report.send(batch.session_id).await.unwrap(),
        other => panic!("expected Batch, got {other:?}"),
    }
}

#[tokio::test]
async fn agent_status_is_capability_gated_and_session_stamped() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (rd, wr) = stream.into_split();
        let mut rd = BufReader::new(rd);
        let mut wr = BufWriter::new(wr);
        let _ = read_frame::<Frame, _>(&mut rd).await.unwrap();
        write_frame(
            &mut wr,
            &build::hello_ack(build::HelloAckArgs {
                protocol_version: PROTOCOL_VERSION_V0,
                writer_id: "test",
                session_id: 333,
                capabilities: CAP_AGENT_STATUS,
                suggested_batch_bytes: 0,
                max_batch_bytes: 0,
                max_inflight_batches: 1,
            }),
        )
        .await
        .unwrap();
        wr.flush().await.unwrap();
        match read_frame::<Frame, _>(&mut rd).await.unwrap().msg {
            FrameMsg::AgentStatus(status) => {
                assert_eq!(status.session_id, 333);
                assert_eq!(status.sequence, 9);
            }
            other => panic!("expected AgentStatus, got {other:?}"),
        }
    });

    let mut client = Client::connect(&addr, [0; 16], "node", SIGNAL_BIT_LOGS, vec![])
        .await
        .unwrap();
    assert!(client.supports_agent_status());
    assert!(client.send_agent_status(9, "{}").await.unwrap());
    server.await.unwrap();
}

#[tokio::test]
async fn combined_capabilities_persist_and_latest_ack_controls_logs_v2_on_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (hello_tx, mut hello_rx) = mpsc::channel(2);
    let (batch_tx, mut batch_rx) = mpsc::channel(2);
    let requested = CAP_STRUCTURED_METRICS_V2 | CAP_LOGS_V2;

    // The first server accepts both requested capabilities; after reconnect it
    // withdraws logs v2 while continuing to accept structured metrics.
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        serve_combined_one(
            first,
            701,
            CAP_STRUCTURED_METRICS_V2 | CAP_LOGS_V2,
            hello_tx.clone(),
            batch_tx.clone(),
        )
        .await;
        let (second, _) = listener.accept().await.unwrap();
        serve_combined_one(second, 702, CAP_STRUCTURED_METRICS_V2, hello_tx, batch_tx).await;
    });

    let mut client = Client::connect_with_protocol(
        &addr,
        [7; 16],
        "combined-host",
        SIGNAL_BIT_METRICS | SIGNAL_BIT_LOGS,
        vec![],
        PROTOCOL_VERSION_V2,
        requested,
    )
    .await
    .unwrap();
    assert_eq!(
        hello_rx.recv().await.unwrap(),
        (PROTOCOL_VERSION_V2, requested)
    );
    assert!(client.supports_structured_metrics());
    assert!(client.supports_logs_v2());
    let mut first_batch = test_batch(1);
    client.send_batch_stamped(&mut first_batch).await.unwrap();
    assert_eq!(batch_rx.recv().await.unwrap(), 701);

    client.reconnect().await.unwrap();
    assert_eq!(
        hello_rx.recv().await.unwrap(),
        (PROTOCOL_VERSION_V2, requested)
    );
    assert!(client.supports_structured_metrics());
    assert!(!client.supports_logs_v2());
    let mut second_batch = test_batch(2);
    client.send_batch_stamped(&mut second_batch).await.unwrap();
    assert_eq!(batch_rx.recv().await.unwrap(), 702);

    server.await.unwrap();
}

#[tokio::test]
async fn latest_ack_can_upgrade_logs_v2_on_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (hello_tx, mut hello_rx) = mpsc::channel(2);
    let (batch_tx, mut batch_rx) = mpsc::channel(2);
    let requested = CAP_STRUCTURED_METRICS_V2 | CAP_LOGS_V2;

    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        serve_combined_one(
            first,
            801,
            CAP_STRUCTURED_METRICS_V2,
            hello_tx.clone(),
            batch_tx.clone(),
        )
        .await;
        let (second, _) = listener.accept().await.unwrap();
        serve_combined_one(
            second,
            802,
            CAP_STRUCTURED_METRICS_V2 | CAP_LOGS_V2,
            hello_tx,
            batch_tx,
        )
        .await;
    });

    let mut client = Client::connect_with_protocol(
        &addr,
        [8; 16],
        "combined-host",
        SIGNAL_BIT_METRICS | SIGNAL_BIT_LOGS,
        vec![],
        PROTOCOL_VERSION_V2,
        requested,
    )
    .await
    .unwrap();
    assert_eq!(
        hello_rx.recv().await.unwrap(),
        (PROTOCOL_VERSION_V2, requested)
    );
    assert!(!client.supports_logs_v2());
    let mut first_batch = test_batch(1);
    client.send_batch_stamped(&mut first_batch).await.unwrap();
    assert_eq!(batch_rx.recv().await.unwrap(), 801);

    client.reconnect().await.unwrap();
    assert_eq!(
        hello_rx.recv().await.unwrap(),
        (PROTOCOL_VERSION_V2, requested)
    );
    assert!(client.supports_logs_v2());
    let mut second_batch = test_batch(2);
    client.send_batch_stamped(&mut second_batch).await.unwrap();
    assert_eq!(batch_rx.recv().await.unwrap(), 802);

    server.await.unwrap();
}

#[tokio::test]
async fn reconnect_restamps_new_session_id() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (tx, mut rx) = mpsc::channel::<u64>(8);

    // Connection #1 hands out session 111 and closes after one batch;
    // connection #2 hands out session 222.
    let server = tokio::spawn(async move {
        let (s1, _) = listener.accept().await.unwrap();
        serve_one(s1, 111, 1, tx.clone()).await;
        let (s2, _) = listener.accept().await.unwrap();
        serve_one(s2, 222, 1, tx.clone()).await;
    });

    let mut client = Client::connect(&addr, [0u8; 16], "test-host", SIGNAL_BIT_LOGS, vec![])
        .await
        .expect("connect");
    assert_eq!(client.session_id(), 111, "first session id from HelloAck");

    // First batch goes over connection #1 and must carry session 111.
    let mut f1 = test_batch(1);
    client
        .send_batch_stamped(&mut f1)
        .await
        .expect("send batch 1");
    assert_eq!(
        rx.recv().await.unwrap(),
        111,
        "batch 1 stamped with session 111"
    );

    // The server has now closed connection #1. Give the client's reader task a
    // moment to observe EOF and drop its ack channel, then confirm the next
    // send is detected as a lost connection rather than silently swallowed.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let mut f2 = test_batch(2);
    assert!(
        client.send_batch_stamped(&mut f2).await.is_err(),
        "send must fail once the server has closed the connection"
    );

    // Reconnect picks up the new server-assigned session, and the resend must
    // be stamped with it — NOT the stale 111.
    client.reconnect().await.expect("reconnect");
    assert_eq!(client.session_id(), 222, "reconnect adopts new session id");
    client
        .send_batch_stamped(&mut f2)
        .await
        .expect("resend batch 2");
    assert_eq!(
        rx.recv().await.unwrap(),
        222,
        "resent batch stamped with session 222"
    );

    server.await.unwrap();
}
