// Updated seek! macro with persistent buffer for handling multiple messages per read
// This generates the chunking code as part of the macro output

// Custom parser for comma-separated type list
struct SeekInput {
    types: syn::punctuated::Punctuated<syn::Path, syn::Token![,]>,
}

impl syn::parse::Parse for SeekInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(Self { types: syn::punctuated::Punctuated::parse_terminated(input)? })
    }
}

#[proc_macro]
pub fn seek(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as SeekInput);
    let paths: Vec<_> = input.types.into_iter().collect();
    let lanes = paths.len();
    if lanes == 0 {
        return syn::Error::new(proc_macro2::Span::call_site(), "seek! requires at least one message type")
            .to_compile_error().into();
    }
    let mut names = std::collections::HashSet::new();
    let mut rx_fields = Vec::new();
    let mut tx_fields = Vec::new();
    let mut input_names = Vec::new();
    let mut output_names = Vec::new();
    for path in &paths {
        let name = &path.segments.last().unwrap().ident;
        let lower = name.to_string().to_lowercase();
        if !names.insert(lower.clone()) {
            return syn::Error::new_spanned(path, "seek! types must have distinct lowercase channel names")
                .to_compile_error().into();
        }
        rx_fields.push(quote::format_ident!("from_dude_{}", lower));
        tx_fields.push(quote::format_ident!("to_dude_{}", lower));
        input_names.push(quote::format_ident!("__seek_input_{}", lower));
        output_names.push(quote::format_ident!("__seek_output_{}", lower));
    }
    let transport: proc_macro2::TokenStream = include_str!("transport.rs").parse()
        .expect("internal seek transport must be valid Rust tokens");
    let output = quote::quote! {
        #transport

        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        pub struct ConnectionState { is_alive: bool, last_ipa: String }
        #[derive(Debug, Clone)]
        pub struct CancelInfo {
            pub cancel: bool,
            pub reason: String,
            pub connection_number: u64,
        }
        pub struct ConnectionHandle {
            pub c_id: u64,
            pub remote_addr: std::net::SocketAddr,
            pub cancel_conn_tasks_sender: tokio::sync::watch::Sender<CancelInfo>,
            pub cancel_conn_tasks_receiver: tokio::sync::watch::Receiver<CancelInfo>,
            #(pub #rx_fields: tokio::sync::mpsc::Receiver<#paths>,)*
            #(pub #tx_fields: tokio::sync::mpsc::Sender<#paths>,)*
        }

        fn __seek_manifest() -> String {
            vec![#(std::any::type_name::<#paths>()),*].join("\n")
        }
        fn __seek_start_connection(
            prepared: __seek_transport::Prepared, c_id: u64,
            sessions: &mut tokio::task::JoinSet<()>,
        ) -> ConnectionHandle {
            let __seek_transport::Prepared { conn, streams } = prepared;
            let remote_addr = conn.remote_address();
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(CancelInfo {
                cancel: false, reason: "Not Canceled".into(), connection_number: c_id,
            });
            #(
                let (#output_names, #rx_fields) = tokio::sync::mpsc::channel::<#paths>(8000);
                let (#tx_fields, #input_names) = tokio::sync::mpsc::channel::<#paths>(8000);
            )*
            let mut tasks = tokio::task::JoinSet::new();
            let mut streams = streams.into_iter();
            #(
                let (tx, rx) = streams.next().expect("validated seek lane count");
                tasks.spawn(__seek_transport::run_rx::<#paths>(rx, #output_names, cancel_rx.clone()));
                tasks.spawn(__seek_transport::run_tx::<#paths>(tx, #input_names, cancel_rx.clone()));
            )*
            let mut guard = __seek_transport::ConnectionGuard::new(conn);
            guard.status = Some((cancel_tx.clone(), c_id));
            sessions.spawn(__seek_transport::supervise(tasks, guard, cancel_rx.clone()));
            ConnectionHandle {
                c_id, remote_addr,
                cancel_conn_tasks_sender: cancel_tx,
                cancel_conn_tasks_receiver: cancel_rx,
                #(#rx_fields,)* #(#tx_fields,)*
            }
        }

        pub struct ServerKernel {
            pub endpoint: quinn::Endpoint,
            pub new_connection_sender: tokio::sync::mpsc::Sender<ConnectionHandle>,
            pub next_c_id: u64,
        }
        impl ServerKernel {
            pub fn new(endpoint: quinn::Endpoint,
                new_connection_sender: tokio::sync::mpsc::Sender<ConnectionHandle>) -> Self
            { Self { endpoint, new_connection_sender, next_c_id: 0 } }

            pub async fn run(&mut self, mut cancel_run: tokio::sync::mpsc::Receiver<()>)
                -> Result<(), Box<dyn std::error::Error>>
            {
                let mut setups = tokio::task::JoinSet::new();
                let mut sessions = tokio::task::JoinSet::new();
                let mut pending = None;
                let mut failed = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_run.recv() => break,
                        _ = self.new_connection_sender.closed() => { failed = true; break; }
                        result = sessions.join_next(), if !sessions.is_empty() => {
                            if let Some(Err(error)) = result { log::error!("seek session failed: {error}"); }
                        }
                        permit = self.new_connection_sender.reserve(), if pending.is_some() => {
                            match permit {
                                Ok(permit) => { permit.send(pending.take().unwrap()); }
                                Err(_) => { failed = true; break; }
                            }
                        }
                        result = setups.join_next(), if !setups.is_empty() && pending.is_none() => {
                            match result {
                                Some(Ok(Ok(prepared))) => {
                                    let id = self.next_c_id;
                                    self.next_c_id = self.next_c_id.checked_add(1)
                                        .ok_or("seek connection id exhausted")?;
                                    pending = Some(__seek_start_connection(prepared, id, &mut sessions));
                                }
                                Some(Ok(Err(error))) => log::warn!("seek rejected connection: {error}"),
                                Some(Err(error)) => log::error!("seek setup task failed: {error}"),
                                None => {},
                            }
                        }
                        incoming = self.endpoint.accept(), if setups.len() < __seek_transport::MAX_PENDING_SETUPS => {
                            let incoming = match incoming { Some(incoming) => incoming, None => break };
                            let manifest = __seek_manifest();
                            setups.spawn(async move {
                                tokio::time::timeout(__seek_transport::SETUP_TIMEOUT, async move {
                                    let conn = incoming.await.map_err(|e| format!("seek TLS handshake: {e}"))?;
                                    __seek_transport::prepare_server(conn, manifest, #lanes).await
                                }).await.map_err(|_| "seek setup timed out".to_owned())?
                            });
                        }
                    }
                }
                self.endpoint.close(0u32.into(), b"seek server stopped");
                if failed { return Err("seek new-connection receiver closed".into()); }
                Ok(())
            }
        }

        type ConnectionRequest = (std::net::SocketAddr, String);
        pub struct ClientKernel {
            pub endpoint: quinn::Endpoint,
            pub connection_request_receiver: tokio::sync::mpsc::Receiver<ConnectionRequest>,
            pub new_connection_sender: tokio::sync::mpsc::Sender<ConnectionHandle>,
            pub next_c_id: u64,
        }
        impl ClientKernel {
            pub fn new(endpoint: quinn::Endpoint,
                connection_request_receiver: tokio::sync::mpsc::Receiver<ConnectionRequest>,
                new_connection_sender: tokio::sync::mpsc::Sender<ConnectionHandle>) -> Self
            { Self { endpoint, connection_request_receiver, new_connection_sender, next_c_id: 0 } }

            pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
                let mut setups = tokio::task::JoinSet::new();
                let mut sessions = tokio::task::JoinSet::new();
                let mut requests_closed = false;
                let mut pending = None;
                loop {
                    if requests_closed && setups.is_empty() && pending.is_none() {
                        break;
                    }
                    tokio::select! {
                        biased;
                        _ = self.new_connection_sender.closed() => break,
                        result = sessions.join_next(), if !sessions.is_empty() => {
                            if let Some(Err(error)) = result { log::error!("seek session failed: {error}"); }
                        }
                        permit = self.new_connection_sender.reserve(), if pending.is_some() => {
                            match permit {
                                Ok(permit) => {
                                    let (prepared, id) = pending.take().unwrap();
                                    permit.send(__seek_start_connection(prepared, id, &mut sessions));
                                }
                                Err(_) => break,
                            }
                        }
                        result = setups.join_next(), if !setups.is_empty() && pending.is_none() => {
                            match result {
                                Some(Ok(Ok((prepared, id)))) => {
                                    pending = Some((prepared, id));
                                }
                                Some(Ok(Err(error))) => log::warn!("seek connection failed: {error}"),
                                Some(Err(error)) => log::error!("seek setup task failed: {error}"),
                                None => {},
                            }
                        }
                        request = self.connection_request_receiver.recv(),
                            if !requests_closed && setups.len() < __seek_transport::MAX_PENDING_SETUPS => {
                            let (address, name) = match request {
                                Some(request) => request,
                                None => { requests_closed = true; continue; }
                            };
                            let id = self.next_c_id;
                            self.next_c_id = self.next_c_id.checked_add(1).ok_or("seek connection id exhausted")?;
                            let endpoint = self.endpoint.clone();
                            let manifest = __seek_manifest();
                            setups.spawn(async move {
                                tokio::time::timeout(__seek_transport::SETUP_TIMEOUT, async move {
                                    let conn = endpoint.connect(address, &name).map_err(|e| e.to_string())?
                                        .await.map_err(|e| format!("seek TLS handshake: {e}"))?;
                                    let prepared = __seek_transport::prepare_client(conn, manifest, #lanes).await?;
                                    Ok::<_, String>((prepared, id))
                                }).await.map_err(|_| "seek setup timed out".to_owned())?
                            });
                        }
                    }
                }
                // Setup mailboxes control dialing, not published connection lifetimes.
                // Only published handles have session tasks. Dropping this run's
                // pending preparations and setups cancels unpublished connections.
                // Active sessions retain their handle-driven cleanup/cancellation.
                sessions.detach_all();
                Ok(())
            }
        }
    };
    output.into()
}