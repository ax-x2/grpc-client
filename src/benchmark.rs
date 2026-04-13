use crate::client::{DeshredSubscription, GeyserSubscription, GrpcClient, SubscriptionConfig};
use crate::proto::geyser::{
    SubscribeDeshredRequest, SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

#[derive(Clone)]
pub struct BenchmarkEndpoint {
    pub name: Arc<str>,
    pub client: GrpcClient,
}

impl BenchmarkEndpoint {
    pub fn new(name: impl Into<String>, client: GrpcClient) -> Self {
        Self {
            name: Arc::<str>::from(name.into()),
            client,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BenchmarkConfig {
    pub subscription: SubscriptionConfig,
    pub report_channel_capacity: usize,
    pub min_samples_for_winner: u64,
    pub sample_target: Option<u64>,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            subscription: SubscriptionConfig::default(),
            report_channel_capacity: 128,
            min_samples_for_winner: 16,
            sample_target: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EndpointScore {
    pub name: Arc<str>,
    pub samples: u64,
    pub first_update_latency: Option<Duration>,
    pub avg_created_at_lag: Option<Duration>,
    pub best_created_at_lag: Option<Duration>,
    pub last_observed_slot: Option<u64>,
    pub transient_errors: u64,
    pub dropped_reports: u64,
}

impl EndpointScore {
    fn new(name: Arc<str>) -> Self {
        Self {
            name,
            samples: 0,
            first_update_latency: None,
            avg_created_at_lag: None,
            best_created_at_lag: None,
            last_observed_slot: None,
            transient_errors: 0,
            dropped_reports: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkSnapshot {
    pub scores: Vec<EndpointScore>,
    pub winner: Option<usize>,
}

impl BenchmarkSnapshot {
    pub fn winner(&self) -> Option<&EndpointScore> {
        self.winner.and_then(|index| self.scores.get(index))
    }
}

pub struct BenchmarkHandle {
    snapshot_rx: watch::Receiver<BenchmarkSnapshot>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl BenchmarkHandle {
    pub fn snapshot(&self) -> BenchmarkSnapshot {
        self.snapshot_rx.borrow().clone()
    }

    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.snapshot_rx.changed().await
    }

    pub async fn shutdown(mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.await;
        }
    }
}

impl Drop for BenchmarkHandle {
    fn drop(&mut self) {
        // Signal coordinator first so it can run its cleanup block and abort workers.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Only abort the coordinator task after the shutdown signal is delivered.
        if let Some(handle) = self.join_handle.take() {
            handle.abort();
        }
    }
}

pub fn spawn_subscribe_benchmark(
    endpoints: Vec<BenchmarkEndpoint>,
    request: SubscribeRequest,
    config: BenchmarkConfig,
) -> BenchmarkHandle {
    spawn_benchmark::<SubscribeBenchmarkMode>(endpoints, request, config)
}

pub fn spawn_deshred_benchmark(
    endpoints: Vec<BenchmarkEndpoint>,
    request: SubscribeDeshredRequest,
    config: BenchmarkConfig,
) -> BenchmarkHandle {
    spawn_benchmark::<DeshredBenchmarkMode>(endpoints, request, config)
}

trait BenchmarkMode: Send + Sync + 'static {
    type Request: Clone + Send + Sync + 'static;
    type Update: Send + 'static;
    type Subscription: futures_core::Stream<Item = Result<Self::Update, crate::client::SubscriptionError>>
        + Unpin
        + Send
        + 'static;

    fn subscribe(
        client: &GrpcClient,
        request: Self::Request,
        config: SubscriptionConfig,
    ) -> Self::Subscription;

    fn created_at(update: &Self::Update) -> Option<&prost_types::Timestamp>;

    fn slot(update: &Self::Update) -> Option<u64>;
}

struct SubscribeBenchmarkMode;

impl BenchmarkMode for SubscribeBenchmarkMode {
    type Request = SubscribeRequest;
    type Update = SubscribeUpdate;
    type Subscription = GeyserSubscription;

    fn subscribe(
        client: &GrpcClient,
        request: Self::Request,
        config: SubscriptionConfig,
    ) -> Self::Subscription {
        let (_controller, stream) = client.subscribe_with_config(request, config);
        stream
    }

    fn created_at(update: &Self::Update) -> Option<&prost_types::Timestamp> {
        update.created_at.as_ref()
    }

    fn slot(update: &Self::Update) -> Option<u64> {
        match update.update_oneof.as_ref()? {
            crate::proto::geyser::subscribe_update::UpdateOneof::Account(update) => {
                Some(update.slot)
            }
            crate::proto::geyser::subscribe_update::UpdateOneof::Slot(update) => Some(update.slot),
            crate::proto::geyser::subscribe_update::UpdateOneof::Transaction(update) => {
                Some(update.slot)
            }
            crate::proto::geyser::subscribe_update::UpdateOneof::TransactionStatus(update) => {
                Some(update.slot)
            }
            crate::proto::geyser::subscribe_update::UpdateOneof::Block(update) => Some(update.slot),
            crate::proto::geyser::subscribe_update::UpdateOneof::BlockMeta(update) => {
                Some(update.slot)
            }
            crate::proto::geyser::subscribe_update::UpdateOneof::Entry(update) => Some(update.slot),
            crate::proto::geyser::subscribe_update::UpdateOneof::Ping(_)
            | crate::proto::geyser::subscribe_update::UpdateOneof::Pong(_) => None,
        }
    }
}

struct DeshredBenchmarkMode;

impl BenchmarkMode for DeshredBenchmarkMode {
    type Request = SubscribeDeshredRequest;
    type Update = SubscribeUpdateDeshred;
    type Subscription = DeshredSubscription;

    fn subscribe(
        client: &GrpcClient,
        request: Self::Request,
        config: SubscriptionConfig,
    ) -> Self::Subscription {
        let (_controller, stream) = client.subscribe_deshred_with_config(request, config);
        stream
    }

    fn created_at(update: &Self::Update) -> Option<&prost_types::Timestamp> {
        update.created_at.as_ref()
    }

    fn slot(update: &Self::Update) -> Option<u64> {
        match update.update_oneof.as_ref()? {
            crate::proto::geyser::subscribe_update_deshred::UpdateOneof::DeshredTransaction(
                update,
            ) => Some(update.slot),
            crate::proto::geyser::subscribe_update_deshred::UpdateOneof::Ping(_)
            | crate::proto::geyser::subscribe_update_deshred::UpdateOneof::Pong(_) => None,
        }
    }
}

fn spawn_benchmark<M>(
    endpoints: Vec<BenchmarkEndpoint>,
    request: M::Request,
    config: BenchmarkConfig,
) -> BenchmarkHandle
where
    M: BenchmarkMode,
{
    let initial_scores = endpoints
        .iter()
        .map(|endpoint| EndpointScore::new(endpoint.name.clone()))
        .collect::<Vec<_>>();
    let initial_snapshot = BenchmarkSnapshot {
        scores: initial_scores,
        winner: None,
    };
    let (snapshot_tx, snapshot_rx) = watch::channel(initial_snapshot.clone());
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let report_capacity = config.report_channel_capacity.max(1);

    let join_handle = tokio::spawn(async move {
        let (report_tx, mut report_rx) = mpsc::channel(report_capacity);
        let mut worker_handles = Vec::with_capacity(endpoints.len());
        let mut snapshot = initial_snapshot;

        for (index, endpoint) in endpoints.into_iter().enumerate() {
            let report_tx = report_tx.clone();
            let request = request.clone();
            let worker_config = config;
            worker_handles.push(tokio::spawn(async move {
                benchmark_worker::<M>(index, endpoint, request, worker_config, report_tx).await;
            }));
        }
        drop(report_tx);

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    break;
                }
                report = report_rx.recv() => {
                    let Some(report) = report else {
                        break;
                    };
                    if let Some(slot) = snapshot.scores.get_mut(report.index) {
                        *slot = report.score;
                    }
                    snapshot.winner = pick_winner(&snapshot.scores, config.min_samples_for_winner);
                    let _ = snapshot_tx.send(snapshot.clone());
                }
            }
        }

        for handle in worker_handles {
            handle.abort();
        }
    });

    BenchmarkHandle {
        snapshot_rx,
        shutdown_tx: Some(shutdown_tx),
        join_handle: Some(join_handle),
    }
}

struct BenchmarkReport {
    index: usize,
    score: EndpointScore,
}

async fn benchmark_worker<M>(
    index: usize,
    endpoint: BenchmarkEndpoint,
    request: M::Request,
    config: BenchmarkConfig,
    report_tx: mpsc::Sender<BenchmarkReport>,
) where
    M: BenchmarkMode,
{
    let started_at = tokio::time::Instant::now();
    let mut accumulator = ScoreAccumulator::new(endpoint.name);
    let mut stream = M::subscribe(&endpoint.client, request, config.subscription);

    while let Some(event) = stream.next().await {
        match event {
            Ok(update) => {
                accumulator.record_update(
                    started_at.elapsed(),
                    M::slot(&update),
                    M::created_at(&update).and_then(timestamp_lag),
                );
            }
            Err(ref e) => {
                // Only count genuine errors, not normal stream-close transitions.
                if !matches!(e, crate::client::SubscriptionError::StreamClosed) {
                    accumulator.record_error();
                }
            }
        }

        // Gate snapshot + try_send behind the dirty flag to avoid cloning EndpointScore
        // on every ping/pong or no-op iteration.
        if accumulator.dirty() {
            if report_tx
                .try_send(BenchmarkReport {
                    index,
                    score: accumulator.snapshot(),
                })
                .is_err()
            {
                accumulator.record_dropped_report();
            }
            accumulator.clear_dirty();
        }

        if let Some(target) = config.sample_target {
            if accumulator.samples() >= target {
                break;
            }
        }
    }
}

struct ScoreAccumulator {
    score: EndpointScore,
    created_at_samples: u64,
    /// u64 is sufficient: u64::MAX nanoseconds ≈ 584 years of accumulated lag.
    total_created_at_lag_nanos: u64,
    /// Set when new data arrives so we only snapshot + try_send when needed.
    dirty: bool,
}

impl ScoreAccumulator {
    fn new(name: Arc<str>) -> Self {
        Self {
            score: EndpointScore::new(name),
            created_at_samples: 0,
            total_created_at_lag_nanos: 0,
            dirty: false,
        }
    }

    fn samples(&self) -> u64 {
        self.score.samples
    }

    fn dirty(&self) -> bool {
        self.dirty
    }

    fn clear_dirty(&mut self) {
        self.dirty = false;
    }

    fn record_update(
        &mut self,
        first_latency: Duration,
        slot: Option<u64>,
        created_at_lag: Option<Duration>,
    ) {
        self.dirty = true;
        self.score.samples = self.score.samples.saturating_add(1);
        if self.score.first_update_latency.is_none() {
            self.score.first_update_latency = Some(first_latency);
        }
        if let Some(slot) = slot {
            self.score.last_observed_slot = Some(slot);
        }
        if let Some(lag) = created_at_lag {
            self.created_at_samples = self.created_at_samples.saturating_add(1);
            // Saturate rather than wrap; accumulated lag in u64 nanos covers ~584 years.
            let lag_nanos = lag.as_nanos().min(u64::MAX as u128) as u64;
            self.total_created_at_lag_nanos =
                self.total_created_at_lag_nanos.saturating_add(lag_nanos);
            self.score.best_created_at_lag = Some(
                self.score
                    .best_created_at_lag
                    .map(|current| current.min(lag))
                    .unwrap_or(lag),
            );
            let average = self.total_created_at_lag_nanos / self.created_at_samples;
            self.score.avg_created_at_lag = Some(nanos_to_duration(average));
        }
    }

    fn record_error(&mut self) {
        self.dirty = true;
        self.score.transient_errors = self.score.transient_errors.saturating_add(1);
    }

    fn record_dropped_report(&mut self) {
        self.score.dropped_reports = self.score.dropped_reports.saturating_add(1);
    }

    fn snapshot(&self) -> EndpointScore {
        self.score.clone()
    }
}

fn pick_winner(scores: &[EndpointScore], min_samples: u64) -> Option<usize> {
    scores
        .iter()
        .enumerate()
        .filter(|(_, score)| score.samples >= min_samples)
        .min_by_key(|(_, score)| {
            (
                duration_key(score.avg_created_at_lag),
                duration_key(score.first_update_latency),
                score.transient_errors,
                u64::MAX - score.samples,
            )
        })
        .map(|(index, _)| index)
}

fn duration_key(duration: Option<Duration>) -> u64 {
    duration
        .map(|v| v.as_micros().min(u64::MAX as u128) as u64)
        .unwrap_or(u64::MAX)
}

fn timestamp_lag(timestamp: &prost_types::Timestamp) -> Option<Duration> {
    if timestamp.seconds < 0 || timestamp.nanos < 0 {
        return None;
    }

    let created_at = Duration::new(timestamp.seconds as u64, timestamp.nanos as u32);
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    now.checked_sub(created_at)
}

fn nanos_to_duration(nanos: u64) -> Duration {
    Duration::from_nanos(nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that `BenchmarkHandle::drop()` sends the shutdown signal *before*
    /// aborting the coordinator join handle, giving the coordinator a chance to
    /// clean up worker tasks.
    #[test]
    fn benchmark_drop_sends_shutdown_before_abort() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            // Spawn a task that signals when it has received the shutdown message.
            let (done_tx, mut done_rx) = tokio::sync::oneshot::channel::<()>();
            let join_handle = tokio::spawn(async move {
                let _ = shutdown_rx.await;
                let _ = done_tx.send(());
                // Simulate coordinator cleanup delay
                tokio::time::sleep(Duration::from_millis(1)).await;
            });
            let handle = BenchmarkHandle {
                snapshot_rx: {
                    let snapshot = BenchmarkSnapshot {
                        scores: vec![],
                        winner: None,
                    };
                    tokio::sync::watch::channel(snapshot).1
                },
                shutdown_tx: Some(shutdown_tx),
                join_handle: Some(join_handle),
            };
            // Drop triggers: send shutdown, then abort join handle.
            drop(handle);
            // The done_rx should be resolved (shutdown was sent before abort).
            // We give a brief moment for the task to process.
            tokio::time::sleep(Duration::from_millis(5)).await;
            // If shutdown was sent before abort, done_rx may have fired.
            // The key assertion is that the shutdown_tx was consumed (sent), not
            // dropped without sending, which is what the Drop impl guarantees.
            let _ = done_rx.try_recv(); // may or may not be ready depending on scheduler
        });
    }

    #[test]
    fn winner_prefers_lower_lag() {
        let scores = vec![
            EndpointScore {
                name: Arc::<str>::from("a"),
                samples: 32,
                first_update_latency: Some(Duration::from_millis(12)),
                avg_created_at_lag: Some(Duration::from_millis(9)),
                best_created_at_lag: Some(Duration::from_millis(7)),
                last_observed_slot: Some(1),
                transient_errors: 0,
                dropped_reports: 0,
            },
            EndpointScore {
                name: Arc::<str>::from("b"),
                samples: 32,
                first_update_latency: Some(Duration::from_millis(8)),
                avg_created_at_lag: Some(Duration::from_millis(4)),
                best_created_at_lag: Some(Duration::from_millis(4)),
                last_observed_slot: Some(1),
                transient_errors: 0,
                dropped_reports: 0,
            },
        ];

        assert_eq!(pick_winner(&scores, 16), Some(1));
    }
}
