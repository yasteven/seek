// Updated seek! macro with persistent buffer for handling multiple messages per read
// This generates the chunking code as part of the macro output

// Custom parser for comma-separated type list
struct SeekInput 
{ types: syn::punctuated::Punctuated<syn::Path, syn::Token![,]>
}

impl syn::parse::Parse for SeekInput 
{ fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> 
  { let types = syn::punctuated::Punctuated::parse_terminated(input)?;
    Ok(SeekInput { types })
  }
}

#[proc_macro]
pub fn seek(input: proc_macro::TokenStream) -> proc_macro::TokenStream 
{ let input = syn::parse_macro_input!(input as SeekInput);
  let struct_names = input.types.clone().into_iter().map
  ( |path| path.segments.last().expect("Path must have at least one segment").ident.clone()
  ).collect::<Vec<_>>();
  let struct_paths = input.types.clone().into_iter().collect::<Vec<_>>();
  let mut outputs = Vec::new();

  // =======================================================
  // CHUNKING IMPLEMENTATION (generated inline per macro invocation)
  // =======================================================
  
  let chunking_module = quote::quote! 
  {
    // Transparent Chunking Layer for QUIC Streams
    // Generated inline as part of seek! macro output
    
    mod __seek_chunking {
      use serde::{Deserialize, Serialize};
      use std::collections::HashMap;
      
      // =======================================================
      // Constants
      // =======================================================
      
      /// Maximum size for a single chunk (safe under QUIC MTU + headers)
      const MAX_CHUNK_SIZE: usize = 50_000;
      
      /// Maximum chunk ID to prevent wraparound issues
      const MAX_CHUNK_ID: u32 = 1_000_000;
      
      // =======================================================
      // Chunk Wrapper Type
      // =======================================================
      
      /// Wrapper enum for chunked or single-packet messages
      #[derive(Debug, Clone, Serialize, Deserialize)]
      pub enum Chunk<T> {
          /// Normal case: entire message fits in one packet (zero overhead)
          Single(T),
          
          /// First chunk of a multi-chunk message
          Start {
              id: u32,
              total_chunks: u32,
              first_chunk: Vec<u8>,
          },
          
          /// Middle chunk of a multi-chunk message
          Middle {
              id: u32,
              chunk_idx: u32,
              total_chunks: u32,
              data: Vec<u8>,
          },
          
          /// Final chunk of a multi-chunk message
          End {
              id: u32,
              chunk_idx: u32,
              total_chunks: u32,
              last_chunk: Vec<u8>,
          },
      }
      
      // =======================================================
      // Chunk Assembly State
      // =======================================================
      
      /// State for reassembling chunked messages
      struct ChunkAssembly {
          total_chunks: u32,
          received_chunks: Vec<Vec<u8>>,
      }
      
      impl ChunkAssembly {
          fn new(total_chunks: u32) -> Self {
              Self {
                  total_chunks,
                  received_chunks: Vec::with_capacity(total_chunks as usize),
              }
          }
          
          fn add_chunk(&mut self, _chunk_idx: u32, data: Vec<u8>) -> bool {
              self.received_chunks.push(data);
              self.received_chunks.len() as u32 == self.total_chunks
          }
          
          fn assemble(self) -> Vec<u8> {
              self.received_chunks.into_iter().flatten().collect()
          }
      }
      
      // =======================================================
      // Sender Functions (for TX path)
      // =======================================================
      
      /// Split a message into chunks if needed
      pub fn prepare_message_chunks<T>(
          message: &T,
          chunk_id_counter: &mut u32,
      ) -> Result<Vec<Vec<u8>>, bincode::Error>
      where
          T: Serialize,
      {
          // First, serialize the raw message to check size
          let raw_serialized = bincode::serialize(message)?;
          let total_size = raw_serialized.len();
          
          // Fast path: message fits in single chunk
          if total_size <= MAX_CHUNK_SIZE {
              let chunk = Chunk::Single(message);
              let serialized = bincode::serialize(&chunk)?;
              log::trace!(
                  "Message fits in single chunk ({} bytes raw, {} bytes with wrapper)",
                  total_size,
                  serialized.len()
              );
              return Ok(vec![serialized]);
          }
          
          // Slow path: need to chunk
          let total_chunks = ((total_size + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE) as u32;
          *chunk_id_counter = chunk_id_counter.wrapping_add(1) % MAX_CHUNK_ID;
          let chunk_id = *chunk_id_counter;
          
          log::debug!(
              "Chunking large message: {} bytes → {} chunks (id={})",
              total_size,
              total_chunks,
              chunk_id
          );
          
          let mut result = Vec::with_capacity(total_chunks as usize);
          let mut offset = 0;
          
          for chunk_idx in 0..total_chunks {
              let end = (offset + MAX_CHUNK_SIZE).min(total_size);
              let chunk_data = raw_serialized[offset..end].to_vec();
              
              let chunk: Chunk<T> = if chunk_idx == 0 {
                  Chunk::Start {
                      id: chunk_id,
                      total_chunks,
                      first_chunk: chunk_data,
                  }
              } else if chunk_idx == total_chunks - 1 {
                  Chunk::End {
                      id: chunk_id,
                      chunk_idx,
                      total_chunks,
                      last_chunk: chunk_data,
                  }
              } else {
                  Chunk::Middle {
                      id: chunk_id,
                      chunk_idx,
                      total_chunks,
                      data: chunk_data,
                  }
              };
              
              let serialized = bincode::serialize(&chunk)?;
              log::trace!(
                  "Prepared chunk {}/{} ({} bytes) for id {}",
                  chunk_idx + 1,
                  total_chunks,
                  serialized.len(),
                  chunk_id
              );
              
              result.push(serialized);
              offset = end;
          }
          
          Ok(result)
      }
      
      // =======================================================
      // Receiver Functions (for RX path)
      // =======================================================
      
      /// State manager for chunk reassembly on receiver side
      pub struct ChunkReceiver<T> {
          assemblies: HashMap<u32, ChunkAssembly>,
          _phantom: std::marker::PhantomData<T>,
      }
      
      impl<T> ChunkReceiver<T>
      where
          T: serde::de::DeserializeOwned + Serialize,
      {
          pub fn new() -> Self {
              Self {
                  assemblies: HashMap::new(),
                  _phantom: std::marker::PhantomData,
              }
          }
          
          /// Process buffer and return (messages, bytes_consumed)
          /// This handles multiple messages concatenated in the buffer
          pub fn process_buffer(&mut self, data: &[u8]) -> Result<(Vec<T>, usize), ChunkError> {
              let mut messages = Vec::new();
              let mut total_consumed = 0;
              let mut remaining = data;
              
              loop {
                  // Try to deserialize from current position
                  let result: Result<Chunk<T>, _> = bincode::deserialize(remaining);
                  
                  match result {
                      Ok(chunk) => {
                          // Figure out how many bytes this chunk consumed
                          let serialized = bincode::serialize(&chunk)
                              .map_err(|e| ChunkError::Deserialization(format!("{:?}", e)))?;
                          let consumed = serialized.len();
                          
                          // Process the chunk
                          match chunk {
                              Chunk::Single(value) => {
                                  log::trace!("Received single-chunk message");
                                  messages.push(value);
                              }
                              Chunk::Start { id, total_chunks, first_chunk } => {
                                  log::debug!("Started assembly id {} (total {} chunks)", id, total_chunks);
                                  let mut assembly = ChunkAssembly::new(total_chunks);
                                  assembly.add_chunk(0, first_chunk);
                                  self.assemblies.insert(id, assembly);
                              }
                              Chunk::Middle { id, chunk_idx, total_chunks, data }
                              | Chunk::End { id, chunk_idx, total_chunks, last_chunk: data } => {
                                  let assembly = self.assemblies.get_mut(&id)
                                      .ok_or_else(|| ChunkError::UnknownChunkId(id))?;
                                  
                                  if assembly.total_chunks != total_chunks {
                                      return Err(ChunkError::TotalChunksMismatch {
                                          id,
                                          expected: assembly.total_chunks,
                                          received: total_chunks,
                                      });
                                  }
                                  
                                  let is_complete = assembly.add_chunk(chunk_idx, data);
                                  
                                  if is_complete {
                                      log::debug!("Completed assembly id {} ({} chunks)", id, total_chunks);
                                      let assembly = self.assemblies.remove(&id).unwrap();
                                      let full_data = assembly.assemble();
                                      let value: T = bincode::deserialize(&full_data)
                                          .map_err(|e| ChunkError::Deserialization(format!("{:?}", e)))?;
                                      messages.push(value);
                                  } else {
                                      log::trace!("Received chunk {}/{} for id {}", chunk_idx + 1, total_chunks, id);
                                  }
                              }
                          }
                          
                          total_consumed += consumed;
                          remaining = &remaining[consumed..];
                          
                          if remaining.is_empty() {
                              break;
                          }
                      }
                      Err(_) => {
                          // Partial message - stop here
                          log::trace!("Partial message, {} bytes remaining in buffer", remaining.len());
                          break;
                      }
                  }
              }
              
              Ok((messages, total_consumed))
          }
          
          /// Clean up stale assemblies (call periodically to prevent memory leaks)
          pub fn cleanup_stale(&mut self, max_assemblies: usize) {
              if self.assemblies.len() > max_assemblies {
                  log::warn!(
                      "Too many pending assemblies ({}), clearing all",
                      self.assemblies.len()
                  );
                  self.assemblies.clear();
              }
          }
      }
      
      // =======================================================
      // Error Types
      // =======================================================
      
      #[derive(Debug)]
      pub enum ChunkError {
          Deserialization(String),
          UnknownChunkId(u32),
          TotalChunksMismatch {
              id: u32,
              expected: u32,
              received: u32,
          },
      }
      
      impl std::fmt::Display for ChunkError {
          fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
              match self {
                  ChunkError::Deserialization(e) => write!(f, "Deserialization error: {}", e),
                  ChunkError::UnknownChunkId(id) => write!(f, "Unknown chunk id: {}", id),
                  ChunkError::TotalChunksMismatch { id, expected, received } => {
                      write!(
                          f,
                          "Total chunks mismatch for id {}: expected {} but received {}",
                          id, expected, received
                      )
                  }
              }
          }
      }
      
      impl std::error::Error for ChunkError {}
    }
  };

  // Add chunking module to outputs first
  outputs.push(chunking_module);

  // =======================================================
  // Rest of the macro (ConnectionHandle, etc.)
  // =======================================================
  
  // ConnectionHandle
  let from_dude_fields = struct_paths.iter().map
  ( |path| 
    { let name = path.segments.last().expect("Wtf path").ident.clone();
      let field_name = syn::Ident::new(&format!("from_dude_{}", name.to_string().to_lowercase()), name.span());
      quote::quote! { , pub #field_name: tokio::sync::mpsc::Receiver<#path> }
    }
  ).collect::<Vec<_>>();
  
  let to_dude_fields = struct_paths.iter().map
  ( |path| 
    { let name = path.segments.last().expect("Wtf path").ident.clone();
      let field_name = syn::Ident::new(&format!("to_dude_{}", name.to_string().to_lowercase()), name.span());
      quote::quote! { , pub #field_name: tokio::sync::mpsc::Sender<#path> }
    }
  ).collect::<Vec<_>>();

  let connection_handle = quote::quote! 
  { 
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct ConnectionState
    { is_alive : bool
    , last_ipa : String
    }
    
    #[derive(Debug, Clone)]
    pub struct CancelInfo 
    { pub cancel: bool
    , pub reason: String
    , pub connection_number: u64
    }

    pub struct ConnectionHandle 
    { pub c_id: u64
    , pub remote_addr: std::net::SocketAddr
    , pub cancel_conn_tasks_sender : tokio::sync::watch::Sender<CancelInfo>
    , pub cancel_conn_tasks_receiver : tokio::sync::watch::Receiver<CancelInfo>
      #(#from_dude_fields)*
      #(#to_dude_fields)*
    }
  };

  // Common arguments and channel creation
  let from_dude_args = struct_paths.iter().map
  ( |path| 
    { let name = path.segments.last().expect("Wtf path").ident.clone();
      let arg_name = syn::Ident::new(&format!("from_dude_{}_sender", name.to_string().to_lowercase()), name.span());
      quote::quote! { , #arg_name: tokio::sync::mpsc::Sender<#path> }
    }
  ).collect::<Vec<_>>();
  
  let to_dude_args = struct_paths.iter().map
  ( |path| 
    { let name = path.segments.last().expect("Wtf path").ident.clone();
      let arg_name = syn::Ident::new(&format!("to_dude_{}_receiver", name.to_string().to_lowercase()), name.span());
      quote::quote! { , #arg_name: tokio::sync::mpsc::Receiver<#path> }
    }
  ).collect::<Vec<_>>();

  let channel_pairs = struct_paths.iter().map
  ( |path| 
    { let name = path.segments.last().expect("Wtf path").ident.clone();
      let from_sender = syn::Ident::new(&format!("from_dude_{}_sender", name.to_string().to_lowercase()), name.span());
      let from_receiver = syn::Ident::new(&format!("from_dude_{}_receiver", name.to_string().to_lowercase()), name.span());
      let to_sender = syn::Ident::new(&format!("to_dude_{}_sender", name.to_string().to_lowercase()), name.span());
      let to_receiver = syn::Ident::new(&format!("to_dude_{}_receiver", name.to_string().to_lowercase()), name.span());
      quote::quote! 
      { let (#from_sender, #from_receiver) = tokio::sync::mpsc::channel(8000);
        let (#to_sender, #to_receiver) = tokio::sync::mpsc::channel(8000);
      }
    }
  ).collect::<Vec<_>>();
  
  let handle_fields = struct_paths.iter().map
  ( |path| 
    { let name = path.segments.last().expect("Wtf path").ident.clone();
      let from_field = syn::Ident::new(&format!("from_dude_{}", name.to_string().to_lowercase()), name.span());
      let to_field = syn::Ident::new(&format!("to_dude_{}", name.to_string().to_lowercase()), name.span());
      let from_receiver = syn::Ident::new(&format!("from_dude_{}_receiver", name.to_string().to_lowercase()), name.span());
      let to_sender = syn::Ident::new(&format!("to_dude_{}_sender", name.to_string().to_lowercase()), name.span());
      quote::quote! 
      { , #from_field: #from_receiver
        , #to_field: #to_sender
      }
    }
  ).collect::<Vec<_>>();
  
  let spawn_args = 
  { let senders = struct_paths.iter().map
    ( |path| 
      { let name = path.segments.last().expect("Wtf path").ident.clone();
        let from_sender = syn::Ident::new(&format!("from_dude_{}_sender", name.to_string().to_lowercase()), name.span());
        quote::quote!{ , #from_sender }
      }
    ).collect::<Vec<_>>();
    let receivers = struct_paths.iter().map
    ( |path| 
      { let name = path.segments.last().expect("Wtf path").ident.clone();
        let to_receiver = syn::Ident::new(&format!("to_dude_{}_receiver", name.to_string().to_lowercase()), name.span());
        quote::quote!{ , #to_receiver }
      }
    ).collect::<Vec<_>>();
    senders.into_iter().chain(receivers.into_iter()).collect::<Vec<_>>()
  };

  // Stream collection code
  let server_stream_gots = struct_names.iter().enumerate().map
  ( |(idx, _)| 
    { let idxp1 = idx + 1;
      quote::quote! 
      { log::debug!("Server accepting bi number {}...", #idxp1);
        match conn.accept_bi().await 
        { 
          Ok((send_stream, recv_stream)) => 
          { let stream_id = send_stream.id().index() as usize;
            log::debug!("Server accepted bi at stream {}...", stream_id);
            streams.push((stream_id, send_stream, recv_stream));
          }
          Err(e) => 
          { log::debug!("Failed to accept stream {}: {:?}", #idxp1, e);
            return;
          }
        }
      }
    }
  ).collect::<Vec<_>>();

  let server_stream_handlers = struct_paths.iter().enumerate().map
  ( |(idx, path)| 
    { let name = path.segments.last().expect("Wtf path").ident.clone();
      let from_sender = syn::Ident::new(&format!("from_dude_{}_sender", name.to_string().to_lowercase()), name.span());
      let to_receiver = syn::Ident::new(&format!("to_dude_{}_receiver", name.to_string().to_lowercase()), name.span());
      let idxp1 = idx + 1;

      quote::quote! 
      { if let Some(pos) = streams.iter().position(|&(id, _, _)| id == #idxp1) 
        { let (_, send_stream, mut recv_stream) = streams.remove(pos);
          let mut cancel_rx_clone = cancel_rx.clone();
          let cancel_tx_clone = cancel_tx.clone();
          let mut buffer = vec![0; 8];
          log::debug!("server_stream_handlers C_ID {} S_ID {} // select await read initial 0th message(=0)", c_id, #idxp1 );
          tokio::select! 
          { read_result = recv_stream.read(&mut buffer) =>
            { match read_result
              { Ok(Some(0)) | Ok(None) => 
                { let reasoning = format!("server_stream_handlers C_ID {} S_ID {} // read() returned 0 or None => the stream is closed", c_id, #idxp1);
                  log::error!("{}", reasoning);
                  let _ = cancel_tx_clone.send
                  ( CancelInfo
                    { cancel : true
                    , reason : reasoning
                    , connection_number : c_id
                    }
                  );
                  return;
                }
                ,
                Ok(Some(n)) => 
                { log::debug!("server_stream_handlers C_ID {} S_ID {} // read initial 0th of {} bytes", c_id, #idxp1, n );
                }
                Err(e) => 
                { let reasoning = format!("server_stream_handlers C_ID {} S_ID {} // STREAM reading error: {:#?}", c_id, #idxp1, e);
                  log::error!("{}",reasoning);
                  let _ = cancel_tx_clone.send
                  ( CancelInfo
                    { cancel : true
                    , reason : reasoning
                    , connection_number : c_id
                    }
                  );
                  return;
                }
              }
            }
            _ = cancel_rx_clone.changed() => 
            { let info = cancel_rx_clone.borrow();
              if info.cancel 
              { log::debug!("server_stream_handlers C_ID {} S_ID {} // Cancellation signal received", c_id, #idxp1);
                return;
              }
            }
          }
          log::debug!("server_stream_handlers C_ID {} S_ID {} // Spawning rx/tx handle tasks...", c_id, #idxp1);
          let cancel_rx_clone = cancel_rx.clone();
          let cancel_tx_clone = cancel_tx.clone();
          task_handles.push
          ( tokio::spawn
            ( Self::server_handle_stream_rx
              ( recv_stream
              , #idxp1 as u64
              , c_id
              , #from_sender.clone()
              , cancel_tx_clone
              , cancel_rx_clone
              )
            )
          );
          let cancel_rx_clone = cancel_rx.clone();
          let cancel_tx_clone = cancel_tx.clone();
          task_handles.push
          ( tokio::spawn
            ( Self::server_handle_stream_tx
              ( send_stream
              , #idxp1 as u64
              , c_id
              , #to_receiver
              , cancel_tx_clone
              , cancel_rx_clone
              )
            )
          );
        } else 
        { log::error!("server_stream_handlers C_ID {} No stream with ID {} found", c_id, #idxp1);
          return;
        }
      }
    }
  ).collect::<Vec<_>>();

  let client_stream_gots = struct_names.iter().enumerate().map
  ( |(idx, _)| 
    { let idxp1 = idx + 1;
      quote::quote! 
      { log::debug!("client_handle_connection C_ID {} // Client opening bi number {}...", c_id, #idxp1);
        match conn.open_bi().await 
        { Ok((send_stream, recv_stream)) => 
          { let stream_id = send_stream.id().index() as u64;
            log::debug!("client_handle_connection C_ID {} Client Opened bi number {} => stream number S_ID = {}", c_id, #idxp1, stream_id);
            streams.push((#idxp1, send_stream, recv_stream));
          }
          Err(e) => 
          { log::error!("client_handle_connection C_ID {} // Failed to open stream {}: {:?}", c_id, #idxp1, e);
            return;
          }
        }
      }
    }
  ).collect::<Vec<_>>();

  let client_stream_handlers = struct_paths.iter().enumerate().map
  ( |(idx, path)| 
    { 
      let name = path.segments.last().expect("Wtf path").ident.clone();
      let from_sender = syn::Ident::new(&format!("from_dude_{}_sender", name.to_string().to_lowercase()), name.span());
      let to_receiver = syn::Ident::new(&format!("to_dude_{}_receiver", name.to_string().to_lowercase()), name.span());
      let idxp1 = idx + 1;
      quote::quote! 
      { if let Some(pos) = streams.iter().position(|&(id, _, _)| id == #idxp1) 
        { let (_, mut send_stream, recv_stream) = streams.remove(pos);
          let cancel_rx_clone = cancel_rx.clone();
          let cancel_tx_clone = cancel_tx.clone();
          // first send a 0
          log::debug!("client_handle_stream_tx C_ID {} // sending initial 0 on stream S_ID = {}", c_id, #idxp1);
          send_stream.write_all(&[0u8]).await.unwrap();
          send_stream.flush().await.unwrap();
          log::debug!("client_handle_stream_tx C_ID {} S_ID {} // Spawning Tasks...", c_id, #idxp1);
          task_handles.push
          ( tokio::spawn
            ( Self::client_handle_stream_rx
              ( recv_stream
              , #idxp1 as u64
              , c_id
              , #from_sender.clone()
              , cancel_tx_clone
              , cancel_rx_clone
              )
            )
          );
          let cancel_rx_clone = cancel_rx.clone();
          let cancel_tx_clone = cancel_tx.clone();
          task_handles.push
          ( tokio::spawn
            ( Self::client_handle_stream_tx
              ( send_stream
              , #idxp1 as u64
              , c_id
              , #to_receiver
              , cancel_tx_clone
              , cancel_rx_clone
              )
            )
          );
        } else 
        { log::error!("No stream with ID {} found for C_ID: {}", #idxp1, c_id);
          return;
        }
      }
    }
  ).collect::<Vec<_>>();

  // Now generate ServerKernel and ClientKernel with chunking-aware stream handlers
  
  let server_kernel = quote::quote! 
  { 
    pub struct ServerKernel 
    { pub endpoint: quinn::Endpoint
    , pub new_connection_sender: tokio::sync::mpsc::Sender<ConnectionHandle>
    , pub next_c_id: u64
    }

    impl ServerKernel 
    {
      pub fn new
      ( endpoint: quinn::Endpoint
      , new_connection_sender: tokio::sync::mpsc::Sender<ConnectionHandle>
      ) -> Self 
      { ServerKernel 
        { endpoint
        , new_connection_sender
        , next_c_id: 0
        }
      }

      async fn server_handle_connection
      ( conn: quinn::Connection
      , c_id: u64
      , mut cancel_tx: tokio::sync::watch::Sender<CancelInfo>
      , mut cancel_rx: tokio::sync::watch::Receiver<CancelInfo>
        #(#from_dude_args)*
        #(#to_dude_args)*
      ) 
      { 
        log::info!("Server Handling connection C_ID = {}", c_id);
        log::debug!("server_handle_connection C_ID {} // spawning task A", c_id);
        let cancel_tx_clone = cancel_tx.clone();
        let mut cancel_rx_clone = cancel_rx.clone();

        let conn_clone = conn.clone();
        tokio::spawn
        ( async move 
          { log::debug!("server_handle_connection C_ID {} // SPAWNED task A",c_id);
            if let Ok(()) = cancel_rx_clone.changed().await 
            { let info = cancel_rx_clone.borrow();
              if info.cancel 
              { log::info!("server_handle_connection C_ID {} task A calling conn.close()", c_id);
                conn_clone.close(0u32.into(), info.reason.as_bytes());
              }
            }
            log::debug!("server_handle_connection C_ID {} // DYING task A",c_id);
          }
        );

        log::debug!("server_handle_connection C_ID {} // Collect streams", c_id);
        let mut streams = Vec::new();
        
        match conn.accept_bi().await 
        { Ok((send_stream, recv_stream)) => 
          { let stream_id = send_stream.id().index() as usize;
            log::debug!("server_handle_connection C_ID: {} // THE FIRST stream accepted", c_id);
            streams.push((stream_id, send_stream, recv_stream));
          }
          Err(e) => 
          { log::debug!("Failed to accept stream EXTRA 0: {:?}", e);
            return;
          }
        }
        
        #(#server_stream_gots)*

        log::debug!("server_handle_connection C_ID {} // Store streams", c_id);
        let mut task_handles = Vec::new();
        
        #(#server_stream_handlers)*
        
        log::info!("Finished handling streams for connection C_ID: {}", c_id);
        let tasks_result = futures::future::join_all(task_handles).await;
        for result in tasks_result 
        { let _ = result;
        }
        log::info!("server_handle_connection C_ID: {} FINISHED!", c_id);
      }

      // =======================================================
      // SERVER STREAM HANDLERS WITH PERSISTENT BUFFER
      // =======================================================

      async fn server_handle_stream_rx<T>
      ( mut recv_stream: quinn::RecvStream
      , s_id: u64
      , c_id: u64
      , sender: tokio::sync::mpsc::Sender<T>
      , mut cancel_tx: tokio::sync::watch::Sender<CancelInfo>
      , mut cancel_rx: tokio::sync::watch::Receiver<CancelInfo>
      )
      where
          T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Send + 'static 
      { log::debug!("server_handle_stream_rx C_ID {} S_ID {} (type={})", c_id, s_id, std::any::type_name::<T>());
        
        let mut read_buffer = vec![0; 65536];
        let mut persistent_buffer = Vec::new(); // Persistent buffer for handling multiple messages
        let mut chunk_receiver = __seek_chunking::ChunkReceiver::<T>::new();
        
        loop 
        { tokio::select! 
          { read_result = recv_stream.read(&mut read_buffer) =>
            { match read_result
              { Ok(Some(0)) | Ok(None) => 
                { let reasoning = format!("server_handle_stream_rx C_ID {} S_ID {} // stream closed", c_id, s_id);
                  log::error!("{}", reasoning);
                  let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                  break;
                }
                Ok(Some(n)) => 
                { // Append new data to persistent buffer
                  persistent_buffer.extend_from_slice(&read_buffer[..n]);
                  
                  log::trace!("server_handle_stream_rx C_ID {} S_ID {} // Read {} bytes, buffer now {} bytes", 
                      c_id, s_id, n, persistent_buffer.len());
                  
                  // Process all complete messages in the buffer
                  match chunk_receiver.process_buffer(&persistent_buffer) 
                  { Ok((messages, consumed)) => 
                    { log::debug!("server_handle_stream_rx C_ID {} S_ID {} // Decoded {} messages, consumed {} bytes", 
                          c_id, s_id, messages.len(), consumed);
                      
                      // Send all decoded messages
                      for decoded in messages 
                      { if let Err(e) = sender.send(decoded).await 
                        { let reasoning = format!("server_handle_stream_rx C_ID {} S_ID {} // mpsc send failed: {:?}", c_id, s_id, e);
                          log::error!("{}", reasoning);
                          let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                          break;
                        }
                      }
                      
                      // Remove consumed bytes from buffer
                      persistent_buffer.drain(..consumed);
                      
                      if !persistent_buffer.is_empty() {
                          log::trace!("server_handle_stream_rx C_ID {} S_ID {} // {} bytes remain in buffer (partial message)", 
                              c_id, s_id, persistent_buffer.len());
                      }
                    }
                    Err(e) => 
                    { let reasoning = format!("server_handle_stream_rx C_ID {} S_ID {} // Chunk error: {:?}", c_id, s_id, e);
                      log::error!("{}", reasoning);
                      let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                      break;
                    }
                  }
                  
                  chunk_receiver.cleanup_stale(100);
                }
                Err(e) => 
                { let reasoning = format!("server_handle_stream_rx C_ID {} S_ID {} // Read error: {:#?}", c_id, s_id, e);
                  log::error!("{}", reasoning);
                  let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                  break;
                }
              }
            }
            _ = cancel_rx.changed() => 
            { let info = cancel_rx.borrow();
              if info.cancel 
              { log::debug!("server_handle_stream_rx C_ID {} S_ID {} // Cancelled", c_id, s_id);
                break;
              }
            }
          }
        }
        log::info!("server_handle_stream_rx C_ID {} S_ID {} ENDED", c_id, s_id);
      }

      async fn server_handle_stream_tx<T: serde::Serialize + Send + 'static>
      ( mut send_stream: quinn::SendStream
      , stream_id: u64
      , c_id: u64
      , mut receiver: tokio::sync::mpsc::Receiver<T>
      , mut cancel_tx: tokio::sync::watch::Sender<CancelInfo>
      , mut cancel_rx: tokio::sync::watch::Receiver<CancelInfo>
      ) 
      { log::debug!("server_handle_stream_tx C_ID {} S_ID {} (type={})", c_id, stream_id, std::any::type_name::<T>());
        
        let mut chunk_id_counter: u32 = 0;
        
        loop 
        { tokio::select!
          { Some(message) = receiver.recv() => 
            { match __seek_chunking::prepare_message_chunks(&message, &mut chunk_id_counter) 
              { Ok(chunks) => 
                { for (idx, chunk_data) in chunks.iter().enumerate() 
                  { match send_stream.write_all(chunk_data).await 
                    { Ok(_) => 
                      { log::debug!("server_handle_stream_tx C_ID {} S_ID {} // OK! for send_stream.write_all(chunk_data).await", c_id, stream_id);
                      }
                      Err(e) => 
                      { let reasoning = format!("server_handle_stream_tx C_ID {} S_ID {} // Write error chunk {}: {:?}", 
                          c_id, stream_id, idx, e);
                        log::error!("{}", reasoning);
                        let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                        return;
                      }
                    }
                  }
                  
                  if let Err(e) = send_stream.flush().await 
                  { let reasoning = format!("server_handle_stream_tx C_ID {} S_ID {} // Flush error: {:?}", c_id, stream_id, e);
                    log::error!("{}", reasoning);
                    let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                    return;
                  }
                  
                  if chunks.len() == 1 
                  { log::debug!("server_handle_stream_tx C_ID {} S_ID {} // Sent single-chunk", c_id, stream_id);
                  } else 
                  { log::debug!("server_handle_stream_tx C_ID {} S_ID {} // Sent {}-chunk message", c_id, stream_id, chunks.len());
                  }
                }
                Err(e) => 
                { let reasoning = format!("server_handle_stream_tx C_ID {} S_ID {} // Chunk prep error: {:?}", c_id, stream_id, e);
                  log::error!("{}", reasoning);
                  let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                  return;
                }
              }
            }
            _ = cancel_rx.changed() => 
            { let info = cancel_rx.borrow(); 
              if info.cancel
              { log::info!("server_handle_stream_tx C_ID {} S_ID {} // Cancelled", c_id, stream_id);
                break;
              }
            }
          }
        }
        log::info!("server_handle_stream_tx C_ID {} S_ID {} ENDED", c_id, stream_id);
      }

      pub async fn run(&mut self, mut cancel_run : tokio::sync::mpsc::Receiver::<()> ) -> Result<(), Box<dyn std::error::Error>> 
      { log::info!("ServerKernel starting run...");
        let epla = format!("{}", self.endpoint.local_addr()?);
        log::info!("Server listening on {}", epla);
        loop
        { tokio::select!
          { res = self.endpoint.accept() =>
            { match res 
              { Some(conn) =>
                { log::info!("Server endpoint {} accepted connection", epla);
                  let conn = conn.await?;
                  let c_id = self.next_c_id;
                  self.next_c_id += 1;
                  let remote_addr = conn.remote_address();
                  log::info!("Server {} New connection from {}, C_ID: {}", epla, remote_addr, c_id);
                  
                  #(#channel_pairs)*
                  
                  let initial_info = CancelInfo 
                  { cancel: false
                  , reason: format!("Not Canceled")
                  , connection_number: c_id
                  };
                  let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(initial_info);
                  let handle = ConnectionHandle 
                  { c_id
                  , remote_addr
                  , cancel_conn_tasks_sender: cancel_tx.clone()
                  , cancel_conn_tasks_receiver: cancel_rx.clone()
                    #(#handle_fields)*
                  };
                  
                  tokio::spawn
                  ( Self::server_handle_connection
                    ( conn
                    , c_id
                    , cancel_tx.clone()
                    , cancel_rx.clone()
                      #(#spawn_args)*
                    )
                  );
                  
                  self.new_connection_sender.send(handle).await?;
                }
              , _ =>
                { log::error!("ServerKernel::run() FAILED in endpoint accept()");
                }
              }
            }
            ,
            _ = cancel_run.recv() =>
            { log::info!("ServerKernel::run() canceling");
              self.endpoint.close(quinn::VarInt::from_u32(0), b"ServerKernel::run() canceled");
              break;
            }
          };
        }
        log::info!("ServerKernel {} // run loop exited", epla);
        Ok(())
      }
    }
  };

  // ClientKernel with persistent buffer
  let client_kernel = quote::quote! 
  { 
    type ConnectionRequest = (std::net::SocketAddr, String);

    pub struct ClientKernel 
    { pub endpoint: quinn::Endpoint
    , pub connection_request_receiver: tokio::sync::mpsc::Receiver<ConnectionRequest>
    , pub new_connection_sender: tokio::sync::mpsc::Sender<ConnectionHandle>
    , pub next_c_id: u64
    }

    impl ClientKernel 
    {
      pub fn new
      ( endpoint: quinn::Endpoint
      , connection_request_receiver: tokio::sync::mpsc::Receiver<ConnectionRequest>
      , new_connection_sender: tokio::sync::mpsc::Sender<ConnectionHandle>
      ) -> Self 
      { ClientKernel 
        { endpoint
        , connection_request_receiver
        , new_connection_sender
        , next_c_id: 0
        }
      }
      
      async fn client_handle_connection
      ( conn: quinn::Connection
      , c_id: u64
      , mut cancel_tx: tokio::sync::watch::Sender<CancelInfo>
      , mut cancel_rx: tokio::sync::watch::Receiver<CancelInfo>
        #(#from_dude_args)*
        #(#to_dude_args)*
      ) 
      { 
        log::info!("Client Handling connection C_ID = {}", c_id);
        
        let conn_clone = conn.clone();
        let mut cancel_rx_clone = cancel_rx.clone();
        tokio::spawn
        ( async move 
          { if let Ok(()) = cancel_rx_clone.changed().await 
            { let info = cancel_rx_clone.borrow();
              if info.cancel 
              { conn_clone.close(0u32.into(), info.reason.as_bytes());
              }
            }
          }
        );

        let mut streams = Vec::new();
        match conn.open_bi().await 
        { Ok((send_stream, recv_stream)) => 
          { streams.push((0, send_stream, recv_stream));
          }
          Err(_) => { return; }
        }
        
        #(#client_stream_gots)*
        
        let mut task_handles = Vec::new();
        
        #(#client_stream_handlers)*
        
        let tasks_result = futures::future::join_all(task_handles).await;
        for result in tasks_result { let _ = result; }
        log::info!("client_handle_connection C_ID: {} FINISHED!", c_id);
      }

      // =======================================================
      // CLIENT STREAM HANDLERS WITH PERSISTENT BUFFER
      // =======================================================

      async fn client_handle_stream_rx<T>
      ( mut recv_stream: quinn::RecvStream
      , s_id: u64
      , c_id: u64
      , sender: tokio::sync::mpsc::Sender<T>
      , mut cancel_tx: tokio::sync::watch::Sender<CancelInfo>
      , mut cancel_rx: tokio::sync::watch::Receiver<CancelInfo>
      )
      where
          T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Send + 'static 
      { log::debug!("client_handle_stream_rx C_ID {} S_ID {} (type={})", c_id, s_id, std::any::type_name::<T>());
        
        let mut read_buffer = vec![0; 65536];
        let mut persistent_buffer = Vec::new(); // Persistent buffer for handling multiple messages
        let mut chunk_receiver = __seek_chunking::ChunkReceiver::<T>::new();
        
        loop 
        { tokio::select! 
          { read_result = recv_stream.read(&mut read_buffer) =>
            { match read_result
              { Ok(Some(0)) | Ok(None) => 
                { let reasoning = format!("client_handle_stream_rx C_ID {} S_ID {} // stream closed", c_id, s_id);
                  log::error!("{}", reasoning);
                  let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                  break;
                }
                Ok(Some(n)) => 
                { // Append new data to persistent buffer
                  persistent_buffer.extend_from_slice(&read_buffer[..n]);
                  
                  log::trace!("client_handle_stream_rx C_ID {} S_ID {} // Read {} bytes, buffer now {} bytes", 
                      c_id, s_id, n, persistent_buffer.len());
                  
                  // Process all complete messages in the buffer
                  match chunk_receiver.process_buffer(&persistent_buffer) 
                  { Ok((messages, consumed)) => 
                    { log::debug!("client_handle_stream_rx C_ID {} S_ID {} // Decoded {} messages, consumed {} bytes", 
                          c_id, s_id, messages.len(), consumed);
                      
                      // Send all decoded messages
                      for decoded in messages 
                      { if let Err(e) = sender.try_send(decoded) 
                        { log::warn!("client_handle_stream_rx C_ID {} S_ID {} // mpsc full: {:?}", c_id, s_id, e);
                        }
                      }
                      
                      // Remove consumed bytes from buffer
                      persistent_buffer.drain(..consumed);
                      
                      if !persistent_buffer.is_empty() {
                          log::trace!("client_handle_stream_rx C_ID {} S_ID {} // {} bytes remain in buffer (partial message)", 
                              c_id, s_id, persistent_buffer.len());
                      }
                    }
                    Err(e) => 
                    { let reasoning = format!("client_handle_stream_rx C_ID {} S_ID {} // Chunk error: {:?}", c_id, s_id, e);
                      log::error!("{}", reasoning);
                      let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                      break;
                    }
                  }
                  
                  chunk_receiver.cleanup_stale(100);
                }
                Err(e) => 
                { let reasoning = format!("client_handle_stream_rx C_ID {} S_ID {} // Read error: {:?}", c_id, s_id, e);
                  log::error!("{}", reasoning);
                  let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                  break;
                }
              }
            }
            _ = cancel_rx.changed() => 
            { if cancel_rx.borrow().cancel 
              { log::debug!("client_handle_stream_rx C_ID {} S_ID {} // Cancelled", c_id, s_id);
                break;
              }
            }
          }
        }
        log::info!("client_handle_stream_rx C_ID {} S_ID {} ENDED", c_id, s_id);
      }

      async fn client_handle_stream_tx<T: serde::Serialize + Send + 'static>
      ( mut send_stream: quinn::SendStream
      , stream_id: u64
      , c_id: u64
      , mut receiver: tokio::sync::mpsc::Receiver<T>
      , mut cancel_tx: tokio::sync::watch::Sender<CancelInfo>
      , mut cancel_rx: tokio::sync::watch::Receiver<CancelInfo>
      ) 
      { log::debug!("client_handle_stream_tx C_ID {} S_ID {} (type={})", c_id, stream_id, std::any::type_name::<T>());
        
        let mut chunk_id_counter: u32 = 0;
        
        loop 
        { tokio::select!
          { Some(message) = receiver.recv() => 
            { match __seek_chunking::prepare_message_chunks(&message, &mut chunk_id_counter) 
              { Ok(chunks) => 
                { for (idx, chunk_data) in chunks.iter().enumerate() 
                  { match send_stream.write_all(chunk_data).await 
                    { Ok(_) => {}
                      Err(e) => 
                      { let reasoning = format!("client_handle_stream_tx C_ID {} S_ID {} // Write error chunk {}: {:?}", 
                          c_id, stream_id, idx, e);
                        log::error!("{}", reasoning);
                        let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                        return;
                      }
                    }
                  }
                  
                  if let Err(e) = send_stream.flush().await 
                  { let reasoning = format!("client_handle_stream_tx C_ID {} S_ID {} // Flush error: {:?}", c_id, stream_id, e);
                    log::error!("{}", reasoning);
                    let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                    return;
                  }
                  
                  if chunks.len() == 1 
                  { log::debug!("client_handle_stream_tx C_ID {} S_ID {} // Sent single-chunk", c_id, stream_id);
                  } else 
                  { log::debug!("client_handle_stream_tx C_ID {} S_ID {} // Sent {} chunks", c_id, stream_id, chunks.len());
                  }
                }
                Err(e) => 
                { let reasoning = format!("client_handle_stream_tx C_ID {} S_ID {} // Chunk prep error: {:?}", c_id, stream_id, e);
                  log::error!("{}", reasoning);
                  let _ = cancel_tx.send(CancelInfo { cancel: true, reason: reasoning, connection_number: c_id });
                  return;
                }
              }
            }
            _ = cancel_rx.changed() => 
            { let info = cancel_rx.borrow(); 
              if info.cancel
              { log::info!("client_handle_stream_tx C_ID {} S_ID {} // Cancelled", c_id, stream_id);
                break;
              }
            }
          }
        }
        log::info!("client_handle_stream_tx C_ID {} S_ID {} ENDED", c_id, stream_id);
      }

      pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> 
      { log::info!("ClientKernel starting run...");
        while let Some((server_addr, server_name)) = self.connection_request_receiver.recv().await 
        { let c_id = self.next_c_id;
          self.next_c_id += 1;
          log::info!("Attempting to connect to {} with name {}", server_addr, server_name);
          match self.endpoint.connect(server_addr, &server_name) 
          { Ok(conn_future) => 
            { match conn_future.await 
              { Ok(conn) => 
                { let remote_addr = conn.remote_address();
                  let epla = format!("{}", self.endpoint.local_addr()?);
                  log::info!("Client {} connected to C_ID {} @ {}", epla, c_id, remote_addr);
                  
                  #(#channel_pairs)*
                  
                  let initial_info = CancelInfo 
                  { cancel: false
                  , reason: format!("Not Canceled")
                  , connection_number: c_id
                  };
                  let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(initial_info);
                  let handle = ConnectionHandle 
                  { c_id
                  , remote_addr
                  , cancel_conn_tasks_sender: cancel_tx.clone()
                  , cancel_conn_tasks_receiver: cancel_rx.clone()
                    #(#handle_fields)*
                  };
                  
                  tokio::spawn
                  ( Self::client_handle_connection
                    ( conn
                    , c_id
                    , cancel_tx.clone()
                    , cancel_rx.clone()
                      #(#spawn_args)*
                    )
                  );
                  
                  self.new_connection_sender.send(handle).await?;
                }
                Err(e) => 
                { log::error!("Connection failed: {:?}", e);
                }
              }
            }
            Err(e) => 
            { log::error!("Failed to initiate connection: {:?}", e);
            }
          }
        }
        log::info!("ClientKernel run loop exited");
        Ok(())
      }
    }
  };

  outputs.push(connection_handle);
  outputs.push(quote::quote! 
  { use tokio::io::AsyncWriteExt;
    #server_kernel
    #client_kernel
  });

  let output = quote::quote! 
  { #(#outputs)*
  };
  
  proc_macro::TokenStream::from(output)
}