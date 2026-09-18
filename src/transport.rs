// Emitted privately by seek!. Application messages remain plain typed values.
mod __seek_transport {
    use bincode::Options;
    use super::CancelInfo;
    use tokio::sync::{mpsc, watch};

    pub(super) const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
    const MAX_MANIFEST_BYTES: usize = 64 * 1024;
    const MAGIC: &[u8; 8] = b"SEEK\0\0\0\x02";
    pub(super) const SETUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    pub(super) const MAX_PENDING_SETUPS: usize = 128;

    pub(super) struct Prepared {
        pub conn: quinn::Connection,
        pub streams: Vec<(quinn::SendStream, quinn::RecvStream)>,
    }

    // Aborting a setup/session also closes its connection and its public status.
    pub(super) struct ConnectionGuard {
        pub conn: quinn::Connection,
        pub status: Option<(watch::Sender<CancelInfo>, u64)>,
        pub reason: String,
        armed: bool,
    }
    impl ConnectionGuard {
        pub fn new(conn: quinn::Connection) -> Self {
            Self { conn, status: None, reason: "seek connection task ended".into(), armed: true }
        }
        pub fn disarm(&mut self) { self.armed = false; }
    }
    impl Drop for ConnectionGuard {
        fn drop(&mut self) {
            if !self.armed { return; }
            if let Some((tx, id)) = &self.status {
                let previous = tx.borrow().clone();
                if previous.cancel {
                    self.reason = previous.reason;
                } else {
                    tx.send_replace(CancelInfo {
                        cancel: true, reason: self.reason.clone(), connection_number: *id,
                    });
                }
            }
            self.conn.close(0u32.into(), self.reason.as_bytes());
        }
    }

    pub(super) async fn cancelled(rx: &mut watch::Receiver<CancelInfo>) {
        loop {
            if rx.borrow().cancel { return; }
            if rx.changed().await.is_err() { return; }
        }
    }

    fn codec() -> impl Options {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_little_endian()
            .with_limit(MAX_MESSAGE_BYTES as u64)
            .reject_trailing_bytes()
    }
    pub(super) fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, String> {
        let body = codec().serialize(value).map_err(|e| format!("seek encode: {e}"))?;
        if body.len() > MAX_MESSAGE_BYTES { return Err("seek message exceeds 64 MiB".into()); }
        Ok(body)
    }
    pub(super) fn decode<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, String> {
        if body.len() > MAX_MESSAGE_BYTES { return Err("seek message exceeds 64 MiB".into()); }
        codec().deserialize(body).map_err(|e| format!("seek invalid frame: {e}"))
    }

    // A read consumes exactly one bounded frame. Never infer consumption by
    // serializing a decoded value, and never retain an invalid complete frame.
    pub(super) async fn read_frame(rx: &mut quinn::RecvStream, limit: usize)
        -> Result<Option<Vec<u8>>, String>
    {
        let mut header = [0u8; 4];
        let first = match rx.read(&mut header).await.map_err(|e| format!("seek read: {e}"))? {
            None => return Ok(None),
            Some(n) => n,
        };
        rx.read_exact(&mut header[first..]).await.map_err(|e| format!("seek truncated header: {e}"))?;
        let len = u32::from_be_bytes(header) as usize;
        if len > limit { return Err(format!("seek frame length {len} exceeds limit {limit}")); }
        let mut body = vec![0; len];
        rx.read_exact(&mut body).await.map_err(|e| format!("seek truncated frame: {e}"))?;
        Ok(Some(body))
    }
    async fn write_frame(tx: &mut quinn::SendStream, body: &[u8]) -> Result<(), quinn::WriteError> {
        tx.write_all(&(body.len() as u32).to_be_bytes()).await?;
        tx.write_all(body).await
    }
    fn stopped_normally(error: &quinn::WriteError) -> bool {
        matches!(error, quinn::WriteError::Stopped(code) if *code == quinn::VarInt::from_u32(0))
    }

    pub(super) async fn run_rx<T: serde::de::DeserializeOwned + Send + 'static>(
        mut rx: quinn::RecvStream, output: mpsc::Sender<T>, mut stop: watch::Receiver<CancelInfo>,
    ) -> Result<(), String> {
        loop {
            let body = tokio::select! {
                biased;
                _ = cancelled(&mut stop) => return Ok(()),
                _ = output.closed() => { let _ = rx.stop(0u32.into()); return Ok(()); }
                result = read_frame(&mut rx, MAX_MESSAGE_BYTES) => match result? {
                    Some(body) => body,
                    None => return Ok(()),
                },
            };
            let message = decode::<T>(&body)?;
            // Backpressure is lossless while connected; shutdown remains live.
            tokio::select! {
                biased;
                _ = cancelled(&mut stop) => return Ok(()),
                result = output.send(message) => if result.is_err() {
                    let _ = rx.stop(0u32.into());
                    return Ok(());
                },
            }
        }
    }
    pub(super) async fn run_tx<T: serde::Serialize + Send + 'static>(
        mut tx: quinn::SendStream, mut input: mpsc::Receiver<T>, mut stop: watch::Receiver<CancelInfo>,
    ) -> Result<(), String> {
        loop {
            let message = tokio::select! {
                biased;
                _ = cancelled(&mut stop) => return Ok(()),
                stopped = tx.stopped() => return match stopped {
                    Ok(None) => Ok(()),
                    Ok(Some(code)) if code == quinn::VarInt::from_u32(0) => Ok(()),
                    other => Err(format!("seek send stream stopped: {other:?}")),
                },
                message = input.recv() => match message {
                    Some(value) => value,
                    None => { let _ = tx.finish(); return Ok(()); }
                },
            };
            let body = encode(&message)?;
            tokio::select! {
                biased;
                _ = cancelled(&mut stop) => return Ok(()),
                result = write_frame(&mut tx, &body) => if let Err(error) = result {
                    if stopped_normally(&error) { return Ok(()); }
                    return Err(format!("seek write: {error}"));
                },
            }
        }
    }

    async fn send_manifest(tx: &mut quinn::SendStream, manifest: &str) -> Result<(), String> {
        if manifest.len() > MAX_MANIFEST_BYTES { return Err("seek type manifest too large".into()); }
        tx.write_all(MAGIC).await.map_err(|e| format!("seek protocol header: {e}"))?;
        write_frame(tx, manifest.as_bytes()).await.map_err(|e| format!("seek type manifest: {e}"))
    }
    async fn check_manifest(rx: &mut quinn::RecvStream, expected: &str) -> Result<(), String> {
        let mut magic = [0u8; 8];
        rx.read_exact(&mut magic).await.map_err(|e| format!("seek protocol header: {e}"))?;
        if &magic != MAGIC { return Err("seek wire protocol mismatch; update both peers".into()); }
        let actual = read_frame(rx, MAX_MANIFEST_BYTES).await?
            .ok_or_else(|| "seek missing type manifest".to_owned())?;
        if actual != expected.as_bytes() {
            return Err("seek ordered type identities differ between peers".into());
        }
        Ok(())
    }
    pub(super) async fn prepare_server(conn: quinn::Connection, manifest: String, lanes: usize)
        -> Result<Prepared, String>
    {
        let mut guard = ConnectionGuard::new(conn.clone());
        let (mut control_tx, mut control_rx) = conn.accept_bi().await.map_err(|e| e.to_string())?;
        check_manifest(&mut control_rx, &manifest).await?;
        send_manifest(&mut control_tx, &manifest).await?;
        let mut streams = Vec::with_capacity(lanes);
        for expected in 0..lanes {
            let (tx, mut rx) = conn.accept_bi().await.map_err(|e| e.to_string())?;
            let mut header = [0u8; 4];
            rx.read_exact(&mut header).await.map_err(|e| format!("seek lane header: {e}"))?;
            if u32::from_be_bytes(header) as usize != expected {
                return Err("seek lane identifier mismatch".into());
            }
            streams.push((tx, rx));
        }
        control_tx.write_all(&[1]).await.map_err(|e| e.to_string())?;
        control_tx.finish().map_err(|e| e.to_string())?;
        guard.disarm();
        Ok(Prepared { conn, streams })
    }
    pub(super) async fn prepare_client(conn: quinn::Connection, manifest: String, lanes: usize)
        -> Result<Prepared, String>
    {
        let mut guard = ConnectionGuard::new(conn.clone());
        let (mut control_tx, mut control_rx) = conn.open_bi().await.map_err(|e| e.to_string())?;
        send_manifest(&mut control_tx, &manifest).await?;
        check_manifest(&mut control_rx, &manifest).await?;
        let mut streams = Vec::with_capacity(lanes);
        for lane in 0..lanes {
            let (mut tx, rx) = conn.open_bi().await.map_err(|e| e.to_string())?;
            // Write immediately; merely opening a QUIC stream does not notify its peer.
            tx.write_all(&(lane as u32).to_be_bytes()).await.map_err(|e| e.to_string())?;
            streams.push((tx, rx));
        }
        let mut ready = [0u8; 1];
        control_rx.read_exact(&mut ready).await.map_err(|e| format!("seek readiness: {e}"))?;
        if ready != [1] { return Err("seek invalid readiness acknowledgement".into()); }
        control_tx.finish().map_err(|e| e.to_string())?;
        guard.disarm();
        Ok(Prepared { conn, streams })
    }

    pub(super) async fn supervise(
        mut tasks: tokio::task::JoinSet<Result<(), String>>,
        mut guard: ConnectionGuard,
        mut stop: watch::Receiver<CancelInfo>,
    ) {
        loop {
            tokio::select! {
                biased;
                _ = cancelled(&mut stop) => break,
                error = guard.conn.closed() => { guard.reason = format!("seek connection closed: {error}"); break; }
                result = tasks.join_next(), if !tasks.is_empty() => match result {
                    Some(Ok(Ok(()))) => {}, // One unused/finished direction need not close other lanes.
                    Some(Ok(Err(error))) => { log::warn!("{error}"); guard.reason = error; break; }
                    Some(Err(error)) => { guard.reason = format!("seek stream task failed: {error}"); log::error!("{}", guard.reason); break; }
                    None => {},
                },
            }
            if tasks.is_empty() { guard.reason = "seek all stream directions closed".into(); break; }
        }
        // JoinSet drop aborts every remaining child, including capacity/write waits.
    }
}
