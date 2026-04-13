use crate::client::{GrpcClient, SubscribeRequestBuilder, SubscriptionConfig};
use crate::proto::geyser::SubscribeRequestFilterSlots;
use crate::proto::geyser::CommitmentLevel;
use crate::types::SlotTracker;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;

pub struct GrpcSlotSubscriber {
    thread_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// owning-thread-only start guard — never shared across threads.
    running: bool,
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
            shutdown_tx: None,
            running: false,
            tracker,
            grpc_url: grpc_url.into().into(),
            commitment,
            cpu_core,
        }
    }

    pub fn start(&mut self) {
        if self.running {
            return;
        }
        self.running = true;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let tracker = Arc::clone(&self.tracker);
        let grpc_url = Arc::clone(&self.grpc_url);
        let commitment = self.commitment;
        let cpu_core = self.cpu_core;

        self.thread_handle = Some(thread::spawn(move || {
            if let Some(core) = cpu_core {
                if let Err(error) = cpu::set_cpu_affinity(vec![core]) {
                    eprintln!("failed to set CPU affinity: {error}");
                }
            }

            run_subscription_thread(tracker, grpc_url, commitment, shutdown_rx);
        }));
    }

    pub fn stop(&mut self) {
        self.running = false;
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }

    pub fn is_connected(&self) -> bool {
        self.tracker.is_connected()
    }

    /// adaptive spin-wait with cpu_pause hints and yield fallback, bounded by timeout.
    /// avoids a fixed 10ms sleep — uses the same strategy as BackoffWait internally.
    pub fn wait_for_connection(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        let mut spins: u32 = 10;
        const MAX_SPINS: u32 = 1000;

        loop {
            if self.tracker.is_connected() {
                return true;
            }
            if start.elapsed() >= timeout {
                return false;
            }

            if spins < MAX_SPINS {
                // spin with PAUSE hint — reduces power and SMT contention.
                for _ in 0..spins {
                    cpu::cpu_pause();
                }
                // exponential backoff toward MAX_SPINS
                spins = spins.saturating_mul(2).min(MAX_SPINS);
            } else {
                // reached max spin budget — yield to the OS scheduler.
                std::thread::yield_now();
            }
        }
    }
}

impl Drop for GrpcSlotSubscriber {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_subscription_thread(
    tracker: Arc<SlotTracker>,
    grpc_url: Arc<str>,
    commitment: CommitmentLevel,
    shutdown_rx: oneshot::Receiver<()>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            tracker.set_connected(false);
            eprintln!("failed to build subscriber runtime: {error}");
            return;
        }
    };

    runtime.block_on(subscription_task(tracker, grpc_url, commitment, shutdown_rx));
}

async fn subscription_task(
    tracker: Arc<SlotTracker>,
    grpc_url: Arc<str>,
    commitment: CommitmentLevel,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let client = match GrpcClient::builder_from_shared(grpc_url.to_string()) {
        Ok(builder) => builder.build(),
        Err(error) => {
            tracker.set_connected(false);
            eprintln!("invalid gRPC endpoint: {error}");
            return;
        }
    };

    let request = SubscribeRequestBuilder::new()
        .commitment(commitment)
        .add_slot_filter(
            "client",
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            },
        )
        .build();

    // use RawSubscription: no SubscribeUpdate allocation on the hot path.
    // reconnect is handled inside SubscriptionCore / RawSubscription.
    let (_controller, mut stream) = client.subscribe_raw(request, SubscriptionConfig::default());

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                tracker.set_connected(false);
                break;
            }
            next = stream.next() => {
                match next {
                    Some(Ok(frame)) => {
                        // set_connected(true) only after the first real message arrives
                        tracker.set_connected(true);
                        // stack-only parse: no heap allocation, no prost decode
                        if let Ok(Some(update)) = frame.parse_slot_update() {
                            tracker.update_raw(update.slot, update.status);
                        }
                        // Ok(None) = ping/pong/non-slot — continue silently
                    }
                    Some(Err(_e)) => {
                        // SubscriptionCore handles reconnect internally; errors
                        // surfaced here are terminal or pre-reconnect notifications.
                        tracker.set_connected(false);
                        // dont break: RawSubscription reconnects automatically.
                    }
                    None => {
                        // stream ended (closed by server or terminal error).
                        tracker.set_connected(false);
                        break;
                    }
                }
            }
        }
    }
}
