use crate::client::GrpcSlotClient;
use crate::proto::geyser::CommitmentLevel;
use crate::types::SlotTracker;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio_stream::StreamExt;

pub struct GrpcSlotSubscriber {
    thread_handle: Option<JoinHandle<()>>,
    running: Arc<AtomicBool>,
    tracker: Arc<SlotTracker>,
    grpc_url: Arc<str>,
    commitment: CommitmentLevel,
    cpu_core: Option<usize>,
}

impl GrpcSlotSubscriber {
    pub fn new(
        grpc_url: impl Into<String>,
        tracker: Arc<SlotTracker>,
        commitment: CommitmentLevel,
        cpu_core: Option<usize>,
    ) -> Self {
        Self {
            thread_handle: None,
            running: Arc::new(AtomicBool::new(false)),
            tracker,
            grpc_url: grpc_url.into().into(),
            commitment,
            cpu_core,
        }
    }

    pub fn start(&mut self) {
        if self.running.swap(true, Ordering::AcqRel) {
            return;
        }

        let running = Arc::clone(&self.running);
        let tracker = Arc::clone(&self.tracker);
        let grpc_url = Arc::clone(&self.grpc_url);
        let commitment = self.commitment;
        let cpu_core = self.cpu_core;

        self.thread_handle = Some(thread::spawn(move || {
            if let Some(core) = cpu_core {
                if let Err(e) = cpu::set_cpu_affinity(vec![core]) {
                    eprintln!("Failed to set CPU affinity: {}", e);
                }
            }

            run_subscription_loop(running, tracker, grpc_url, commitment);
        }));
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Release);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }

    pub fn is_connected(&self) -> bool {
        self.tracker.is_connected()
    }

    pub fn wait_for_connection(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if self.tracker.is_connected() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }
}

impl Drop for GrpcSlotSubscriber {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_subscription_loop(
    running: Arc<AtomicBool>,
    tracker: Arc<SlotTracker>,
    grpc_url: Arc<str>,
    commitment: CommitmentLevel,
) {
    let mut backoff = Duration::from_millis(100);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    while running.load(Ordering::Acquire) {
        tracker.set_connected(false);

        match connect_and_subscribe(&running, &tracker, &grpc_url, commitment) {
            Ok(()) => {
                backoff = Duration::from_millis(100);
            }
            Err(e) => {
                eprintln!("gRPC error: {}, reconnecting in {:?}", e, backoff);
                std::thread::sleep(backoff);
                backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
            }
        }
    }
}

fn connect_and_subscribe(
    running: &Arc<AtomicBool>,
    tracker: &Arc<SlotTracker>,
    grpc_url: &str,
    commitment: CommitmentLevel,
) -> Result<(), Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(async { subscribe_async(running, tracker, grpc_url, commitment).await })
}

async fn subscribe_async(
    running: &Arc<AtomicBool>,
    tracker: &Arc<SlotTracker>,
    grpc_url: &str,
    commitment: CommitmentLevel,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GrpcSlotClient::connect(grpc_url).await?;
    let mut stream = client.subscribe_slots(commitment).await?;

    tracker.set_connected(true);

    while running.load(Ordering::Acquire) {
        match stream.next().await {
            Some(Ok(update)) => {
                client.process_message(update, tracker);
            }
            Some(Err(e)) => {
                return Err(Box::new(e));
            }
            None => {
                return Err("Stream closed".into());
            }
        }
    }

    Ok(())
}
