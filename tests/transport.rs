#![allow(dead_code)]
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Command(pub u64);
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Reply(pub String);

mod normal {
    use super::{Command, Reply};
    see_k::seek!(Command, Reply);
    pub fn manifest() -> String { __seek_manifest() }
    pub async fn raw_prepare(conn: quinn::Connection) -> Result<Vec<(quinn::SendStream, quinn::RecvStream)>, String> {
        __seek_transport::prepare_client(conn, __seek_manifest(), 2).await.map(|prepared| prepared.streams)
    }
    pub async fn transmit(tx: quinn::SendStream, input: tokio::sync::mpsc::Receiver<Reply>, stop: tokio::sync::watch::Receiver<CancelInfo>) -> Result<(), String> {
        __seek_transport::run_tx(tx, input, stop).await
    }
    pub async fn prepare(conn: quinn::Connection) -> Result<(), String> {
        __seek_transport::prepare_client(conn, __seek_manifest(), 2).await.map(|_| ())
    }
    #[test]
    fn normalized_values_have_explicit_frame_boundaries() {
        fn trim<'de, D: serde::Deserializer<'de>>(de: D) -> Result<String, D::Error> {
            Ok(<String as serde::Deserialize>::deserialize(de)?.trim().to_owned())
        }
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Normalized(#[serde(deserialize_with = "trim")] String);
        let body = __seek_transport::encode(&Normalized(" hello ".into())).unwrap();
        let value: Normalized = __seek_transport::decode(&body).unwrap();
        assert_eq!(value.0, "hello");
    }
    #[test]
    fn malformed_and_trailing_bytes_are_errors() {
        #[derive(serde::Deserialize)] enum Tag { A }
        assert!(__seek_transport::decode::<Tag>(&[255; 4]).is_err());
        let mut valid = __seek_transport::encode(&Command(9)).unwrap();
        valid.push(0);
        assert!(__seek_transport::decode::<Command>(&valid).is_err());
    }
    pub async fn read_frame(rx: &mut quinn::RecvStream) -> Result<Option<Vec<u8>>, String> {
        __seek_transport::read_frame(rx, __seek_transport::MAX_MESSAGE_BYTES).await
    }
}
mod reversed {
    use super::{Command, Reply};
    see_k::seek!(Reply, Command);
}

fn endpoints() -> (quinn::Endpoint, quinn::Endpoint) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert = certified.cert.der().clone();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der());
    let config = quinn::ServerConfig::with_single_cert(vec![cert.clone()], key.into()).unwrap();
    let server = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).unwrap();
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap());
    (server, client)
}

struct Pair {
    server: normal::ConnectionHandle,
    client: normal::ConnectionHandle,
    stop: mpsc::Sender<()>,
    server_task: tokio::task::JoinHandle<Result<(), String>>,
}
async fn pair() -> Pair {
    let (server, client) = endpoints();
    let address = server.local_addr().unwrap();
    let (new_tx, mut new_rx) = mpsc::channel(4);
    let (client_tx, mut client_rx) = mpsc::channel(4);
    let (request_tx, request_rx) = mpsc::channel(4);
    let (stop, stop_rx) = mpsc::channel(1);
    let mut sk = normal::ServerKernel::new(server, new_tx);
    let mut ck = normal::ClientKernel::new(client, request_rx, client_tx);
    let server_task = tokio::spawn(async move { sk.run(stop_rx).await.map_err(|e| e.to_string()) });
    let client_task = tokio::spawn(async move { ck.run().await.map_err(|e| e.to_string()) });
    request_tx.send((address, "localhost".into())).await.unwrap();
    drop(request_tx); // Closing only the dialing mailbox must preserve active handles.
    let server = timeout(Duration::from_secs(5), new_rx.recv()).await.unwrap().unwrap();
    let client = timeout(Duration::from_secs(5), client_rx.recv()).await.unwrap().unwrap();
    client_task.await.unwrap().unwrap();
    // Keep the server's new-handle mailbox open until this test ends.
    tokio::spawn(async move { while new_rx.recv().await.is_some() {} });
    Pair { server, client, stop, server_task }
}

#[tokio::test]
async fn public_api_destructuring_and_unused_directions() {
    let Pair { server, client, stop, server_task } = pair().await;
    let normal::ConnectionHandle {
        mut from_dude_command, to_dude_reply, ..
    } = server;
    let normal::ConnectionHandle {
        to_dude_command, mut from_dude_reply, ..
    } = client;
    for id in 0..30 {
        to_dude_command.send(Command(id)).await.unwrap();
        assert_eq!(timeout(Duration::from_secs(3), from_dude_command.recv()).await.unwrap(), Some(Command(id)));
        to_dude_reply.send(Reply(format!("reply {id}"))).await.unwrap();
        assert_eq!(timeout(Duration::from_secs(3), from_dude_reply.recv()).await.unwrap(), Some(Reply(format!("reply {id}"))));
    }
    stop.send(()).await.unwrap();
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn large_messages_and_immediate_first_message() {
    let mut p = pair().await;
    p.client.to_dude_command.send(Command(42)).await.unwrap();
    assert_eq!(timeout(Duration::from_secs(3), p.server.from_dude_command.recv()).await.unwrap(), Some(Command(42)));
    for len in [0, 49_999, 50_001, 150_000, 2_000_000] {
        let value = Reply("x".repeat(len));
        p.server.to_dude_reply.send(value).await.unwrap();
        let got = timeout(Duration::from_secs(5), p.client.from_dude_reply.recv()).await.unwrap().unwrap();
        assert_eq!(got.0.len(), len);
    }
    p.stop.send(()).await.unwrap();
    p.server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn full_client_mailbox_preserves_all_messages() {
    let mut p = pair().await;
    // More than the public mailbox capacity, without consuming replies yet.
    for id in 0..8_100 { p.server.to_dude_reply.send(Reply(id.to_string())).await.unwrap(); }
    timeout(Duration::from_secs(10), async {
        while p.client.from_dude_reply.len() < 8000 { tokio::task::yield_now().await; }
    }).await.unwrap();
    for id in 0..8_100 {
        let got = timeout(Duration::from_secs(5), p.client.from_dude_reply.recv()).await.unwrap().unwrap();
        assert_eq!(got.0, id.to_string());
    }
    p.stop.send(()).await.unwrap();
    p.server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancellation_works_with_full_destination_mailbox() {
    let p = pair().await;
    for id in 0..8_100 { p.client.to_dude_command.send(Command(id)).await.unwrap(); }
    timeout(Duration::from_secs(10), async {
        while p.server.from_dude_command.len() < 8000 { tokio::task::yield_now().await; }
    }).await.unwrap();
    tokio::task::yield_now().await;
    p.server.cancel_conn_tasks_sender.send(normal::CancelInfo {
        cancel: true, reason: "test stop".into(), connection_number: p.server.c_id,
    }).unwrap();
    timeout(Duration::from_secs(3), p.client.to_dude_command.closed()).await.unwrap();
    p.stop.send(()).await.unwrap();
    p.server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn failed_tls_and_stalled_setup_do_not_stop_other_connections() {
    let (server, client) = endpoints();
    let address = server.local_addr().unwrap();
    let (new_tx, mut new_rx) = mpsc::channel(4);
    let (stop_tx, stop_rx) = mpsc::channel(1);
    let mut sk = normal::ServerKernel::new(server, new_tx);
    let task = tokio::spawn(async move { sk.run(stop_rx).await.map_err(|e| e.to_string()) });
    let good_config = {
        // Keep a trusted client endpoint before replacing the original's roots.
        client.clone()
    };
    // A certificate-name mismatch fails TLS without altering shared endpoint config.
    assert!(client.connect(address, "wrong-name.invalid").unwrap().await.is_err());
    assert!(!task.is_finished());
    let stalled = good_config.connect(address, "localhost").unwrap().await.unwrap();
    // This peer completes TLS but never sends a seek protocol header.
    let good = client.connect(address, "localhost").unwrap().await.unwrap();
    let prepare = tokio::spawn(normal::prepare(good));
    let handle = timeout(Duration::from_secs(5), new_rx.recv()).await.unwrap().unwrap();
    prepare.await.unwrap().unwrap();
    drop(handle);
    stop_tx.send(()).await.unwrap();
    timeout(Duration::from_secs(2), task).await.unwrap().unwrap().unwrap();
    timeout(Duration::from_secs(2), stalled.closed()).await.unwrap();
}

#[tokio::test]
async fn reordered_types_rejected_before_handle_delivery() {
    let (server, client) = endpoints();
    let address = server.local_addr().unwrap();
    let (new_tx, mut new_rx) = mpsc::channel(1);
    let (client_tx, mut client_rx) = mpsc::channel(1);
    let (request_tx, request_rx) = mpsc::channel(1);
    let (stop, stop_rx) = mpsc::channel(1);
    let mut sk = normal::ServerKernel::new(server, new_tx);
    let mut ck = reversed::ClientKernel::new(client, request_rx, client_tx);
    let server_task = tokio::spawn(async move { sk.run(stop_rx).await.map_err(|e| e.to_string()) });
    let client_task = tokio::spawn(async move { ck.run().await.map_err(|e| e.to_string()) });
    request_tx.send((address, "localhost".into())).await.unwrap();
    drop(request_tx);
    timeout(Duration::from_secs(5), client_task).await.unwrap().unwrap().unwrap();
    assert!(client_rx.recv().await.is_none());
    assert!(new_rx.try_recv().is_err());
    stop.send(()).await.unwrap();
    server_task.await.unwrap().unwrap();
}

async fn raw_pair() -> (quinn::Endpoint, quinn::Endpoint, quinn::Connection, quinn::Connection) {
    let (server, client) = endpoints();
    let dialing = client.connect(server.local_addr().unwrap(), "localhost").unwrap();
    let accepting = async { server.accept().await.unwrap().await.unwrap() };
    let (client_conn, server_conn) = tokio::join!(dialing, accepting);
    (server, client, server_conn, client_conn.unwrap())
}

#[tokio::test]
async fn bounded_framing_handles_fragmentation_coalescing_and_invalid_lengths() {
    let (_server, _client, server_conn, client_conn) = raw_pair().await;
    let (mut tx, _rx) = client_conn.open_bi().await.unwrap();
    tx.write_all(&[0, 0]).await.unwrap();
    let (_tx, mut rx) = server_conn.accept_bi().await.unwrap();
    let writer = tokio::spawn(async move {
        tx.write_all(&[0, 3, 10]).await.unwrap();
        tokio::task::yield_now().await;
        tx.write_all(&[11, 12, 0, 0, 0, 1, 13]).await.unwrap();
        tx.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
        tx.finish().unwrap();
    });
    assert_eq!(normal::read_frame(&mut rx).await.unwrap(), Some(vec![10, 11, 12]));
    assert_eq!(normal::read_frame(&mut rx).await.unwrap(), Some(vec![13]));
    assert!(normal::read_frame(&mut rx).await.unwrap_err().contains("exceeds limit"));
    writer.await.unwrap();
}

#[tokio::test]
async fn full_new_connection_queue_does_not_block_shutdown() {
    let (server, client) = endpoints();
    let address = server.local_addr().unwrap();
    let (new_tx, _new_rx) = mpsc::channel(1);
    let (stop_tx, stop_rx) = mpsc::channel(1);
    let mut sk = normal::ServerKernel::new(server, new_tx);
    let task = tokio::spawn(async move { sk.run(stop_rx).await.map_err(|e| e.to_string()) });
    let mut peers = Vec::new();
    for _ in 0..3 {
        let conn = client.connect(address, "localhost").unwrap().await.unwrap();
        peers.push(conn.clone());
        timeout(Duration::from_secs(5), normal::prepare(conn)).await.unwrap().unwrap();
    }
    stop_tx.send(()).await.unwrap();
    timeout(Duration::from_secs(2), task).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn one_stalled_dial_does_not_block_another() {
    let (server, client) = endpoints();
    let (stalled_endpoint, _unused_client) = endpoints();
    let address = server.local_addr().unwrap();
    let (new_tx, mut new_rx) = mpsc::channel(4);
    let (client_tx, mut client_rx) = mpsc::channel(4);
    let (request_tx, request_rx) = mpsc::channel(4);
    let (stop, stop_rx) = mpsc::channel(1);
    let mut sk = normal::ServerKernel::new(server, new_tx);
    let mut ck = normal::ClientKernel::new(client, request_rx, client_tx);
    let server_task = tokio::spawn(async move { sk.run(stop_rx).await.map_err(|e| e.to_string()) });
    let client_task = tokio::spawn(async move { ck.run().await.map_err(|e| e.to_string()) });
    request_tx.send((stalled_endpoint.local_addr().unwrap(), "localhost".into())).await.unwrap();
    request_tx.send((address, "localhost".into())).await.unwrap();
    let handle = timeout(Duration::from_secs(5), client_rx.recv()).await.unwrap().unwrap();
    assert_eq!(handle.c_id, 1);
    assert!(timeout(Duration::from_secs(5), new_rx.recv()).await.unwrap().is_some());
    stop.send(()).await.unwrap();
    server_task.await.unwrap().unwrap();
    client_task.abort();
    let _ = client_task.await;
}

#[tokio::test]
async fn incompatible_wire_version_is_rejected_without_handle() {
    let (server, client) = endpoints();
    let address = server.local_addr().unwrap();
    let (new_tx, mut new_rx) = mpsc::channel(1);
    let (stop, stop_rx) = mpsc::channel(1);
    let mut sk = normal::ServerKernel::new(server, new_tx);
    let task = tokio::spawn(async move { sk.run(stop_rx).await.map_err(|e| e.to_string()) });
    let conn = client.connect(address, "localhost").unwrap().await.unwrap();
    let (mut tx, _rx) = conn.open_bi().await.unwrap();
    tx.write_all(b"SEEK\0\0\0\x01").await.unwrap();
    timeout(Duration::from_secs(3), conn.closed()).await.unwrap();
    assert!(new_rx.try_recv().is_err());
    stop.send(()).await.unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn malformed_complete_frame_cancels_connection_and_notifies_handle() {
    let (server, client) = endpoints();
    let address = server.local_addr().unwrap();
    let (new_tx, mut new_rx) = mpsc::channel(1);
    let (stop, stop_rx) = mpsc::channel(1);
    let mut sk = normal::ServerKernel::new(server, new_tx);
    let task = tokio::spawn(async move { sk.run(stop_rx).await.map_err(|e| e.to_string()) });
    let conn = client.connect(address, "localhost").unwrap().await.unwrap();
    let mut streams = normal::raw_prepare(conn.clone()).await.unwrap();
    let mut handle = new_rx.recv().await.unwrap();
    // Command requires a u64; this is a complete but invalid one-byte body.
    streams[0].0.write_all(&[0, 0, 0, 1, 255]).await.unwrap();
    timeout(Duration::from_secs(3), conn.closed()).await.unwrap();
    timeout(Duration::from_secs(3), handle.cancel_conn_tasks_receiver.wait_for(|info| info.cancel)).await.unwrap().unwrap();
    assert!(handle.cancel_conn_tasks_receiver.borrow().reason.contains("invalid frame"));
    assert!(timeout(Duration::from_secs(3), handle.from_dude_command.recv()).await.unwrap().is_none());
    stop.send(()).await.unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn blocked_network_write_is_cancellable() {
    let (_server, _client, server_conn, client_conn) = raw_pair().await;
    let (mut tx, _rx) = client_conn.open_bi().await.unwrap();
    tx.write_all(&[0]).await.unwrap();
    let (_peer_tx, _unread_peer_rx) = server_conn.accept_bi().await.unwrap();
    let (input, input_rx) = mpsc::channel(1);
    let (stop, stop_rx) = tokio::sync::watch::channel(normal::CancelInfo {
        cancel: false, reason: String::new(), connection_number: 0,
    });
    // Exceeds Quinn's normal send/receive windows; peer deliberately never reads.
    input.send(Reply("x".repeat(32 * 1024 * 1024))).await.unwrap();
    drop(input); // If the write completed, the transmitter would finish.
    let mut task = tokio::spawn(normal::transmit(tx, input_rx, stop_rx));
    assert!(timeout(Duration::from_millis(100), &mut task).await.is_err());
    stop.send(normal::CancelInfo { cancel: true, reason: "stop".into(), connection_number: 0 }).unwrap();
    timeout(Duration::from_secs(3), task).await.unwrap().unwrap().unwrap();
}
