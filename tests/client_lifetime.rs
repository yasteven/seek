use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Message(pub String);
mod wire { use super::Message; see_k::seek!(Message); }

const DEADLINE: Duration = Duration::from_secs(5);
type KernelTask = tokio::task::JoinHandle<Result<(), String>>;

fn endpoints() -> (quinn::Endpoint, quinn::Endpoint) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
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

// MSTA and USTA's AIHO connector return just the established handle and task.
// Both temporary control mailboxes disappear on return from this function.
async fn connect_once(client: quinn::Endpoint, address: std::net::SocketAddr)
    -> (wire::ConnectionHandle, KernelTask)
{
    let (requests, request_rx) = mpsc::channel(1);
    let (connections, mut connection_rx) = mpsc::channel(1);
    let mut kernel = wire::ClientKernel::new(client, request_rx, connections);
    let task = tokio::spawn(async move { kernel.run().await.map_err(|e| e.to_string()) });
    requests.send((address, "localhost".into())).await.unwrap();
    let handle = timeout(DEADLINE, connection_rx.recv()).await.unwrap().unwrap();
    (handle, task)
}

async fn exchange(client: &mut wire::ConnectionHandle, server: &mut wire::ConnectionHandle) {
    for id in 0..3 {
        let request = format!("request {id}: Nicole — café Ελληνικά 你好 日本語 🙂");
        client.to_dude_message.send(Message(request.clone())).await.unwrap();
        assert_eq!(timeout(DEADLINE, server.from_dude_message.recv()).await.unwrap(), Some(Message(request)));
        let reply = format!("reply {id}: ✅ connection remains usable");
        server.to_dude_message.send(Message(reply.clone())).await.unwrap();
        assert_eq!(timeout(DEADLINE, client.from_dude_message.recv()).await.unwrap(), Some(Message(reply)));
    }
}

#[tokio::test]
async fn connect_helper_return_preserves_connection_and_explicit_cancellation() {
    let (server, client) = endpoints();
    let address = server.local_addr().unwrap();
    let (connections, mut connection_rx) = mpsc::channel(1);
    let (stop, stop_rx) = mpsc::channel(1);
    let mut kernel = wire::ServerKernel::new(server, connections);
    let server_task = tokio::spawn(async move { kernel.run(stop_rx).await.map_err(|e| e.to_string()) });
    let (mut client, client_task) = connect_once(client, address).await;
    let mut server = timeout(DEADLINE, connection_rx.recv()).await.unwrap().unwrap();

    // Wait for the dialer to finish BEFORE sending any application messages.
    timeout(DEADLINE, client_task).await.unwrap().unwrap().expect("dropping setup mailboxes must stop dialing normally");
    exchange(&mut client, &mut server).await;
    client.cancel_conn_tasks_sender.send_replace(wire::CancelInfo {
        cancel: true, reason: "application finished".into(), connection_number: client.c_id,
    });
    timeout(DEADLINE, server.cancel_conn_tasks_receiver.wait_for(|info| info.cancel)).await.unwrap().unwrap();
    assert!(timeout(DEADLINE, client.from_dude_message.recv()).await.unwrap().is_none());
    stop.send(()).await.unwrap();
    timeout(DEADLINE, server_task).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn dropping_handle_receiver_preserves_session_even_with_dial_sender_alive() {
    let (server, client) = endpoints();
    let address = server.local_addr().unwrap();
    let (connections, mut connection_rx) = mpsc::channel(1);
    let (stop, stop_rx) = mpsc::channel(1);
    let mut sk = wire::ServerKernel::new(server, connections);
    let server_task = tokio::spawn(async move { sk.run(stop_rx).await.map_err(|e| e.to_string()) });
    let (requests, request_rx) = mpsc::channel(1);
    let (delivered, mut delivered_rx) = mpsc::channel(1);
    let mut ck = wire::ClientKernel::new(client, request_rx, delivered);
    let client_task = tokio::spawn(async move { ck.run().await.map_err(|e| e.to_string()) });
    requests.send((address, "localhost".into())).await.unwrap();
    let mut client = timeout(DEADLINE, delivered_rx.recv()).await.unwrap().unwrap();
    let mut server = timeout(DEADLINE, connection_rx.recv()).await.unwrap().unwrap();
    drop(delivered_rx);
    timeout(DEADLINE, client_task).await.unwrap().unwrap().expect("handle delivery channel is not session ownership");
    assert!(requests.is_closed());
    exchange(&mut client, &mut server).await;
    // Detached sessions must also clean up when their application handle goes away.
    drop(client);
    timeout(DEADLINE, server.cancel_conn_tasks_receiver.wait_for(|info| info.cancel)).await.unwrap().unwrap();
    drop(requests);
    stop.send(()).await.unwrap();
    timeout(DEADLINE, server_task).await.unwrap().unwrap().unwrap();
}
