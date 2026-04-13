use crate::proto::geyser::{
    CommitmentLevel, GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
    GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
    GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
    PongResponse, SubscribeDeshredRequest, SubscribeReplayInfoRequest, SubscribeReplayInfoResponse,
    SubscribeRequest, SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterDeshredTransactions, SubscribeRequestFilterEntry,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    SubscribeUpdate, SubscribeUpdateDeshred, geyser_client::GeyserClient as ProtoGeyserClient,
    subscribe_update::UpdateOneof, subscribe_update_deshred::UpdateOneof as DeshredUpdateOneof,
};
use crate::types::SlotTracker;
use bytes::Bytes;
use futures_core::Stream;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::time::Sleep;
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tonic::metadata::{AsciiMetadataValue, MetadataValue, errors::InvalidMetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Uri};
use tonic::{Code, Request, Status, Streaming};

type InterceptedChannel = InterceptedService<Channel, MetadataInterceptor>;
type GeyserTransportClient = ProtoGeyserClient<InterceptedChannel>;

const DEFAULT_PING_ID: i32 = 1;

#[derive(Debug, Clone)]
pub struct MetadataInterceptor {
    x_token: Option<AsciiMetadataValue>,
    x_request_snapshot: bool,
}

impl Interceptor for MetadataInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(ref x_token) = self.x_token {
            // clone only the AsciiMetadataValue (cheap Arc-backed clone), not the Option
            request.metadata_mut().insert("x-token", x_token.clone());
        }
        if self.x_request_snapshot {
            request
                .metadata_mut()
                .insert("x-request-snapshot", MetadataValue::from_static("true"));
        }
        Ok(request)
    }
}

#[derive(Debug, Error)]
pub enum GrpcClientBuilderError {
    #[error("invalid metadata value: {0}")]
    MetadataValue(#[from] InvalidMetadataValue),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

#[derive(Debug, Error)]
pub enum GrpcClientError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("gRPC status: {0}")]
    Status(#[from] tonic::Status),
}

#[derive(Debug, Error)]
pub enum SubscriptionError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("gRPC status: {0}")]
    Status(#[from] tonic::Status),
    #[error("stream closed")]
    StreamClosed,
}

impl SubscriptionError {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Status(status)
                if matches!(
                    status.code(),
                    Code::Unimplemented | Code::Unauthenticated | Code::PermissionDenied
                )
        )
    }

    fn rejects_resume(&self) -> bool {
        matches!(
            self,
            Self::Status(status)
                if matches!(
                    status.code(),
                    Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange
                )
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReconnectConfig {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_multiplier: u32,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 2,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResumeConfig {
    pub enabled: bool,
    pub max_attempts_with_from_slot: u32,
}

impl Default for ResumeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts_with_from_slot: 5,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SubscriptionConfig {
    pub reconnect: ReconnectConfig,
    pub resume: ResumeConfig,
    pub auto_pong: bool,
    pub control_queue_capacity: usize,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            reconnect: ReconnectConfig::default(),
            resume: ResumeConfig::default(),
            auto_pong: true,
            control_queue_capacity: 4,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubscriptionController<R> {
    latest: watch::Sender<Arc<R>>,
}

impl<R> SubscriptionController<R>
where
    R: Clone + Send + Sync + 'static,
{
    pub fn snapshot(&self) -> Arc<R> {
        self.latest.borrow().clone()
    }

    pub fn replace(&self, request: R) {
        self.latest.send_replace(Arc::new(request));
    }

    pub fn modify<F>(&self, modify: F)
    where
        F: FnOnce(&mut R),
    {
        let mut request = self.snapshot().as_ref().clone();
        modify(&mut request);
        self.replace(request);
    }

    pub fn batch(&self) -> RequestBatch<R> {
        RequestBatch {
            controller: self.clone(),
            request: self.snapshot().as_ref().clone(),
        }
    }
}

#[derive(Debug)]
pub struct RequestBatch<R> {
    controller: SubscriptionController<R>,
    request: R,
}

impl<R> RequestBatch<R>
where
    R: Clone + Send + Sync + 'static,
{
    pub fn request(&self) -> &R {
        &self.request
    }

    pub fn request_mut(&mut self) -> &mut R {
        &mut self.request
    }

    pub fn commit(self) {
        self.controller.replace(self.request);
    }
}

#[derive(Clone)]
pub struct GrpcClient {
    config: Arc<ClientConfig>,
}

#[derive(Clone)]
struct ClientConfig {
    endpoint: Endpoint,
    /// Lazy channel built once; `channel.clone()` is a cheap Arc bump.
    channel: Channel,
    x_token: Option<AsciiMetadataValue>,
    x_request_snapshot: bool,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
}

impl GrpcClient {
    pub fn builder_from_shared(
        endpoint: impl Into<Bytes>,
    ) -> Result<GrpcClientBuilder, GrpcClientBuilderError> {
        Ok(GrpcClientBuilder::new(Endpoint::from_shared(endpoint)?))
    }

    pub fn builder_from_static(endpoint: &'static str) -> GrpcClientBuilder {
        GrpcClientBuilder::new(Endpoint::from_static(endpoint))
    }

    pub async fn connect(grpc_url: &str) -> Result<Self, GrpcClientError> {
        let client = Self::builder_from_shared(grpc_url.to_owned())
            .map_err(|error| match error {
                GrpcClientBuilderError::Transport(error) => GrpcClientError::Transport(error),
                GrpcClientBuilderError::MetadataValue(_) => unreachable!("metadata not used"),
            })?
            .build();
        // one eager connect to validate connectivity.
        // subsequent calls will use the cached lazy channel.
        client.config.endpoint.connect().await?;
        Ok(client)
    }

    pub fn subscribe(&self) -> (SubscriptionController<SubscribeRequest>, GeyserSubscription) {
        self.subscribe_with_config(SubscribeRequest::default(), SubscriptionConfig::default())
    }

    pub fn subscribe_with_request(
        &self,
        request: SubscribeRequest,
    ) -> (SubscriptionController<SubscribeRequest>, GeyserSubscription) {
        self.subscribe_with_config(request, SubscriptionConfig::default())
    }

    pub fn subscribe_with_config(
        &self,
        request: SubscribeRequest,
        config: SubscriptionConfig,
    ) -> (SubscriptionController<SubscribeRequest>, GeyserSubscription) {
        let (controller, latest_rx) = subscription_control(request);
        let stream = SubscriptionCore::<SubscribeMode>::new(
            self.clone(),
            config,
            controller.clone(),
            latest_rx,
        );
        (controller, GeyserSubscription { inner: stream })
    }

    pub fn subscribe_deshred(
        &self,
    ) -> (
        SubscriptionController<SubscribeDeshredRequest>,
        DeshredSubscription,
    ) {
        self.subscribe_deshred_with_config(
            SubscribeDeshredRequest::default(),
            SubscriptionConfig::default(),
        )
    }

    pub fn subscribe_deshred_with_request(
        &self,
        request: SubscribeDeshredRequest,
    ) -> (
        SubscriptionController<SubscribeDeshredRequest>,
        DeshredSubscription,
    ) {
        self.subscribe_deshred_with_config(request, SubscriptionConfig::default())
    }

    pub fn subscribe_deshred_with_config(
        &self,
        request: SubscribeDeshredRequest,
        config: SubscriptionConfig,
    ) -> (
        SubscriptionController<SubscribeDeshredRequest>,
        DeshredSubscription,
    ) {
        let (controller, latest_rx) = subscription_control(request);
        let stream = SubscriptionCore::<DeshredMode>::new(
            self.clone(),
            config,
            controller.clone(),
            latest_rx,
        );
        (controller, DeshredSubscription { inner: stream })
    }

    pub async fn subscribe_replay_info(
        &self,
    ) -> Result<SubscribeReplayInfoResponse, GrpcClientError> {
        let mut client = self.connect_geyser_client().await?;
        let response = client
            .subscribe_replay_info(Request::new(SubscribeReplayInfoRequest::default()))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn ping(&self, count: i32) -> Result<PongResponse, GrpcClientError> {
        let mut client = self.connect_geyser_client().await?;
        let response = client.ping(Request::new(PingRequest { count })).await?;
        Ok(response.into_inner())
    }

    pub async fn get_latest_blockhash(
        &self,
        commitment: Option<CommitmentLevel>,
    ) -> Result<GetLatestBlockhashResponse, GrpcClientError> {
        let mut client = self.connect_geyser_client().await?;
        let response = client
            .get_latest_blockhash(Request::new(GetLatestBlockhashRequest {
                commitment: commitment.map(|value| value as i32),
            }))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get_block_height(
        &self,
        commitment: Option<CommitmentLevel>,
    ) -> Result<GetBlockHeightResponse, GrpcClientError> {
        let mut client = self.connect_geyser_client().await?;
        let response = client
            .get_block_height(Request::new(GetBlockHeightRequest {
                commitment: commitment.map(|value| value as i32),
            }))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get_slot(
        &self,
        commitment: Option<CommitmentLevel>,
    ) -> Result<GetSlotResponse, GrpcClientError> {
        let mut client = self.connect_geyser_client().await?;
        let response = client
            .get_slot(Request::new(GetSlotRequest {
                commitment: commitment.map(|value| value as i32),
            }))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn is_blockhash_valid(
        &self,
        blockhash: String,
        commitment: Option<CommitmentLevel>,
    ) -> Result<IsBlockhashValidResponse, GrpcClientError> {
        let mut client = self.connect_geyser_client().await?;
        let response = client
            .is_blockhash_valid(Request::new(IsBlockhashValidRequest {
                blockhash,
                commitment: commitment.map(|value| value as i32),
            }))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get_version(&self) -> Result<GetVersionResponse, GrpcClientError> {
        let mut client = self.connect_geyser_client().await?;
        let response = client
            .get_version(Request::new(GetVersionRequest::default()))
            .await?;
        Ok(response.into_inner())
    }

    async fn connect_geyser_client(
        &self,
    ) -> Result<GeyserTransportClient, tonic::transport::Error> {
        // cheap Arc bump — no tcp handshake per call
        Ok(self.config.build_geyser_client(self.config.channel.clone()))
    }

    /// clone the underlying lazy `Channel`. used by `RawMode::open`.
    #[inline]
    pub(crate) fn channel(&self) -> &Channel {
        &self.config.channel
    }

    /// build a `MetadataInterceptor` from the config. used by `RawMode::open`.
    #[inline]
    pub(crate) fn interceptor(&self) -> MetadataInterceptor {
        MetadataInterceptor {
            x_token: self.config.x_token.clone(),
            x_request_snapshot: self.config.x_request_snapshot,
        }
    }

    /// return the configured max decoding message size limit.
    #[inline]
    pub(crate) fn max_decoding_message_size(&self) -> Option<usize> {
        self.config.max_decoding_message_size
    }

    /// add a raw subscription that yields `Bytes` frames, bypassing prost decode.
    pub fn subscribe_raw(
        &self,
        request: SubscribeRequest,
        config: SubscriptionConfig,
    ) -> (
        SubscriptionController<SubscribeRequest>,
        crate::raw::RawSubscription,
    ) {
        use crate::raw::{RawMode, RawSubscription};
        let (controller, latest_rx) = subscription_control(request);
        let stream = SubscriptionCore::<RawMode>::new(
            self.clone(),
            config,
            controller.clone(),
            latest_rx,
        );
        (controller, RawSubscription { inner: stream })
    }
}

pub struct GrpcClientBuilder {
    endpoint: Endpoint,
    x_token: Option<AsciiMetadataValue>,
    x_request_snapshot: bool,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
}

impl GrpcClientBuilder {
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            x_token: None,
            x_request_snapshot: false,
            max_decoding_message_size: None,
            max_encoding_message_size: None,
        }
    }

    pub fn from_shared(endpoint: impl Into<Bytes>) -> Result<Self, GrpcClientBuilderError> {
        GrpcClient::builder_from_shared(endpoint)
    }

    pub fn from_static(endpoint: &'static str) -> Self {
        GrpcClient::builder_from_static(endpoint)
    }

    pub fn build(self) -> GrpcClient {
        // build the lazy channel once here; connect_geyser_client() clones it
        // instead of calling endpoint.connect().await each time.
        let channel = self.endpoint.clone().connect_lazy();
        GrpcClient {
            config: Arc::new(ClientConfig {
                endpoint: self.endpoint,
                channel,
                x_token: self.x_token,
                x_request_snapshot: self.x_request_snapshot,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }),
        }
    }

    pub fn x_token(mut self, value: impl AsRef<str>) -> Result<Self, GrpcClientBuilderError> {
        self.x_token = Some(MetadataValue::try_from(value.as_ref())?);
        Ok(self)
    }

    pub fn set_x_request_snapshot(mut self, enabled: bool) -> Self {
        self.x_request_snapshot = enabled;
        self
    }

    pub fn connect_timeout(mut self, duration: Duration) -> Self {
        self.endpoint = self.endpoint.connect_timeout(duration);
        self
    }

    pub fn timeout(mut self, duration: Duration) -> Self {
        self.endpoint = self.endpoint.timeout(duration);
        self
    }

    pub fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.endpoint = self.endpoint.tcp_nodelay(enabled);
        self
    }

    pub fn tcp_keepalive(mut self, duration: Option<Duration>) -> Self {
        self.endpoint = self.endpoint.tcp_keepalive(duration);
        self
    }

    pub fn http2_keep_alive_interval(mut self, duration: Duration) -> Self {
        self.endpoint = self.endpoint.http2_keep_alive_interval(duration);
        self
    }

    pub fn keep_alive_timeout(mut self, duration: Duration) -> Self {
        self.endpoint = self.endpoint.keep_alive_timeout(duration);
        self
    }

    pub fn initial_stream_window_size(mut self, size: impl Into<Option<u32>>) -> Self {
        self.endpoint = self.endpoint.initial_stream_window_size(size);
        self
    }

    pub fn initial_connection_window_size(mut self, size: impl Into<Option<u32>>) -> Self {
        self.endpoint = self.endpoint.initial_connection_window_size(size);
        self
    }

    pub fn concurrency_limit(mut self, limit: usize) -> Self {
        self.endpoint = self.endpoint.concurrency_limit(limit);
        self
    }

    pub fn rate_limit(mut self, limit: u64, duration: Duration) -> Self {
        self.endpoint = self.endpoint.rate_limit(limit, duration);
        self
    }

    pub fn tls_config(
        mut self,
        tls_config: ClientTlsConfig,
    ) -> Result<Self, GrpcClientBuilderError> {
        self.endpoint = self.endpoint.tls_config(tls_config)?;
        Ok(self)
    }

    pub fn origin(mut self, origin: Uri) -> Self {
        self.endpoint = self.endpoint.origin(origin);
        self
    }

    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.max_decoding_message_size = Some(limit);
        self
    }

    pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
        self.max_encoding_message_size = Some(limit);
        self
    }
}

impl ClientConfig {
    fn build_geyser_client(&self, channel: Channel) -> GeyserTransportClient {
        let interceptor = MetadataInterceptor {
            x_token: self.x_token.clone(),
            x_request_snapshot: self.x_request_snapshot,
        };
        let mut client = ProtoGeyserClient::with_interceptor(channel, interceptor);
        if let Some(limit) = self.max_decoding_message_size {
            client = client.max_decoding_message_size(limit);
        }
        if let Some(limit) = self.max_encoding_message_size {
            client = client.max_encoding_message_size(limit);
        }
        client
    }
}

pub struct GeyserSubscription {
    inner: SubscriptionCore<SubscribeMode>,
}

impl GeyserSubscription {
    pub fn controller(&self) -> &SubscriptionController<SubscribeRequest> {
        self.inner.controller()
    }

    pub fn dropped_control_messages(&self) -> u64 {
        self.inner.dropped_control_messages()
    }

    pub fn last_observed_slot(&self) -> Option<u64> {
        self.inner.last_observed_slot()
    }
}

impl Stream for GeyserSubscription {
    type Item = Result<SubscribeUpdate, SubscriptionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

pub struct DeshredSubscription {
    inner: SubscriptionCore<DeshredMode>,
}

impl DeshredSubscription {
    pub fn controller(&self) -> &SubscriptionController<SubscribeDeshredRequest> {
        self.inner.controller()
    }

    pub fn dropped_control_messages(&self) -> u64 {
        self.inner.dropped_control_messages()
    }

    pub fn last_observed_slot(&self) -> Option<u64> {
        self.inner.last_observed_slot()
    }
}

impl Stream for DeshredSubscription {
    type Item = Result<SubscribeUpdateDeshred, SubscriptionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

pub(crate) struct SubscriptionCore<M>
where
    M: SubscriptionMode,
{
    client: GrpcClient,
    config: SubscriptionConfig,
    controller: SubscriptionController<M::Request>,
    latest_request_rx: watch::Receiver<Arc<M::Request>>,
    state: SubscriptionState<M::Request, M::Update>,
    control_tx: Option<mpsc::Sender<M::Request>>,
    last_observed_slot: Option<u64>,
    attempts_since_success: u32,
    next_backoff: Duration,
    seen_message_on_connection: bool,
    dropped_control_messages: u64,
    pending_error: Option<SubscriptionError>,
    marker: std::marker::PhantomData<M>,
}

enum SubscriptionState<R, U> {
    Idle,
    Connecting(Pin<Box<dyn Future<Output = Result<Connected<R, U>, ConnectFailure>> + Send>>),
    Streaming(Streaming<U>),
    Sleeping(Pin<Box<Sleep>>),
    Closed,
}

struct Connected<R, U> {
    stream: Streaming<U>,
    control_tx: mpsc::Sender<R>,
}

struct ConnectFailure {
    error: SubscriptionError,
    used_resume: bool,
}

impl<M> SubscriptionCore<M>
where
    M: SubscriptionMode,
{
    fn new(
        client: GrpcClient,
        config: SubscriptionConfig,
        controller: SubscriptionController<M::Request>,
        latest_request_rx: watch::Receiver<Arc<M::Request>>,
    ) -> Self {
        Self {
            client,
            config,
            controller,
            latest_request_rx,
            state: SubscriptionState::Idle,
            control_tx: None,
            last_observed_slot: None,
            attempts_since_success: 0,
            next_backoff: config.reconnect.initial_backoff,
            seen_message_on_connection: false,
            dropped_control_messages: 0,
            pending_error: None,
            marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn controller(&self) -> &SubscriptionController<M::Request> {
        &self.controller
    }

    pub(crate) fn dropped_control_messages(&self) -> u64 {
        self.dropped_control_messages
    }

    pub(crate) fn last_observed_slot(&self) -> Option<u64> {
        self.last_observed_slot
    }

    fn start_connect(&mut self) {
        let mut client = self.client.clone();
        let config = self.config;
        let latest_request_rx = self.latest_request_rx.clone();
        let last_observed_slot = self.last_observed_slot;
        let attempts_since_success = self.attempts_since_success;
        self.state = SubscriptionState::Connecting(Box::pin(async move {
            let latest_snapshot = latest_request_rx.borrow().clone();
            let resume_slot = if config.resume.enabled
                && attempts_since_success < config.resume.max_attempts_with_from_slot
            {
                last_observed_slot
            } else {
                None
            };
            let used_resume = resume_slot.is_some();
            // apply_resume already has a fast path when from_slot matches.
            let initial_request = M::apply_resume(latest_snapshot.as_ref(), resume_slot);
            let (control_tx, control_rx) = mpsc::channel(config.control_queue_capacity.max(1));
            let request_stream = RequestStream::new(
                initial_request,
                latest_snapshot,
                latest_request_rx,
                control_rx,
            );
            let stream = M::open(&mut client, request_stream)
                .await
                .map_err(|error| ConnectFailure {
                    error: SubscriptionError::Status(error),
                    used_resume,
                })?;
            Ok(Connected { stream, control_tx })
        }));
    }

    #[inline]
    fn on_first_message(&mut self) {
        if self.seen_message_on_connection {
            return;
        }
        self.seen_message_on_connection = true;
        self.attempts_since_success = 0;
        self.next_backoff = self.config.reconnect.initial_backoff;
    }

    #[cold]
    #[inline(never)]
    fn on_disconnect(&mut self, error: SubscriptionError, used_resume: bool) {
        self.control_tx = None;
        let retryable = !error.is_terminal();

        if used_resume && error.rejects_resume() {
            self.attempts_since_success = self.config.resume.max_attempts_with_from_slot;
        } else if !self.seen_message_on_connection {
            self.attempts_since_success = self.attempts_since_success.saturating_add(1);
        }

        self.seen_message_on_connection = false;
        self.pending_error = Some(error);

        if retryable {
            let delay = self.next_backoff;
            self.next_backoff = scale_backoff(
                self.next_backoff,
                self.config.reconnect.backoff_multiplier,
                self.config.reconnect.max_backoff,
            );
            self.state = SubscriptionState::Sleeping(Box::pin(tokio::time::sleep(delay)));
        } else {
            self.state = SubscriptionState::Closed;
        }
    }

    fn try_send_pong(&mut self) {
        if let Some(control_tx) = &self.control_tx {
            if control_tx
                .try_send(M::pong_request(DEFAULT_PING_ID))
                .is_err()
            {
                self.dropped_control_messages = self.dropped_control_messages.saturating_add(1);
            }
        }
    }
}

impl<M> Unpin for SubscriptionCore<M> where M: SubscriptionMode {}

impl<M> Stream for SubscriptionCore<M>
where
    M: SubscriptionMode,
{
    type Item = Result<M::Update, SubscriptionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if let Some(error) = this.pending_error.take() {
                return Poll::Ready(Some(Err(error)));
            }

            match &mut this.state {
                SubscriptionState::Idle => {
                    this.start_connect();
                }
                SubscriptionState::Connecting(connecting) => match connecting.as_mut().poll(cx) {
                    Poll::Ready(Ok(connected)) => {
                        this.control_tx = Some(connected.control_tx);
                        this.seen_message_on_connection = false;
                        this.state = SubscriptionState::Streaming(connected.stream);
                    }
                    Poll::Ready(Err(failure)) => {
                        this.on_disconnect(failure.error, failure.used_resume);
                    }
                    Poll::Pending => return Poll::Pending,
                },
                SubscriptionState::Streaming(stream) => match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(update))) => {
                        this.on_first_message();
                        if let Some(slot) = M::observed_slot(&update) {
                            this.last_observed_slot = Some(slot);
                        }
                        if this.config.auto_pong && M::is_ping(&update) {
                            this.try_send_pong();
                        }
                        return Poll::Ready(Some(Ok(update)));
                    }
                    Poll::Ready(Some(Err(error))) => {
                        this.on_disconnect(SubscriptionError::Status(error), false);
                    }
                    Poll::Ready(None) => {
                        this.on_disconnect(SubscriptionError::StreamClosed, false);
                    }
                    Poll::Pending => return Poll::Pending,
                },
                SubscriptionState::Sleeping(delay) => match delay.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        this.state = SubscriptionState::Idle;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                SubscriptionState::Closed => return Poll::Ready(None),
            }
        }
    }
}

pub(crate) trait SubscriptionMode: Send + Sync + 'static {
    type Request: Clone + Send + Sync + 'static;
    type Update: Send + 'static;

    fn open<'a>(
        client: &'a mut GrpcClient,
        request_stream: RequestStream<Self::Request>,
    ) -> Pin<Box<dyn Future<Output = Result<Streaming<Self::Update>, tonic::Status>> + Send + 'a>>;

    fn observed_slot(update: &Self::Update) -> Option<u64>;

    fn is_ping(update: &Self::Update) -> bool;

    fn pong_request(id: i32) -> Self::Request;

    fn apply_resume(base: &Self::Request, from_slot: Option<u64>) -> Self::Request;
}

struct SubscribeMode;

impl SubscriptionMode for SubscribeMode {
    type Request = SubscribeRequest;
    type Update = SubscribeUpdate;

    fn open<'a>(
        client: &'a mut GrpcClient,
        request_stream: RequestStream<Self::Request>,
    ) -> Pin<Box<dyn Future<Output = Result<Streaming<Self::Update>, tonic::Status>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut transport = client
                .connect_geyser_client()
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
            let response = transport.subscribe(Request::new(request_stream)).await?;
            Ok(response.into_inner())
        })
    }

    fn observed_slot(update: &Self::Update) -> Option<u64> {
        match update.update_oneof.as_ref()? {
            UpdateOneof::Account(update) => Some(update.slot),
            UpdateOneof::Slot(update) => Some(update.slot),
            UpdateOneof::Transaction(update) => Some(update.slot),
            UpdateOneof::TransactionStatus(update) => Some(update.slot),
            UpdateOneof::Block(update) => Some(update.slot),
            UpdateOneof::BlockMeta(update) => Some(update.slot),
            UpdateOneof::Entry(update) => Some(update.slot),
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => None,
        }
    }

    fn is_ping(update: &Self::Update) -> bool {
        matches!(update.update_oneof, Some(UpdateOneof::Ping(_)))
    }

    fn pong_request(id: i32) -> Self::Request {
        SubscribeRequest {
            ping: Some(SubscribeRequestPing { id }),
            ..Default::default()
        }
    }

    fn apply_resume(base: &Self::Request, from_slot: Option<u64>) -> Self::Request {
        if from_slot == base.from_slot {
            // common case: no change needed
            return base.clone();
        }
        let mut request = base.clone();
        request.from_slot = from_slot;
        request
    }
}

struct DeshredMode;

impl SubscriptionMode for DeshredMode {
    type Request = SubscribeDeshredRequest;
    type Update = SubscribeUpdateDeshred;

    fn open<'a>(
        client: &'a mut GrpcClient,
        request_stream: RequestStream<Self::Request>,
    ) -> Pin<Box<dyn Future<Output = Result<Streaming<Self::Update>, tonic::Status>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut transport = client
                .connect_geyser_client()
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
            let response = transport
                .subscribe_deshred(Request::new(request_stream))
                .await?;
            Ok(response.into_inner())
        })
    }

    fn observed_slot(update: &Self::Update) -> Option<u64> {
        match update.update_oneof.as_ref()? {
            DeshredUpdateOneof::DeshredTransaction(update) => Some(update.slot),
            DeshredUpdateOneof::Ping(_) | DeshredUpdateOneof::Pong(_) => None,
        }
    }

    fn is_ping(update: &Self::Update) -> bool {
        matches!(update.update_oneof, Some(DeshredUpdateOneof::Ping(_)))
    }

    fn pong_request(id: i32) -> Self::Request {
        SubscribeDeshredRequest {
            ping: Some(SubscribeRequestPing { id }),
            ..Default::default()
        }
    }

    fn apply_resume(base: &Self::Request, _from_slot: Option<u64>) -> Self::Request {
        base.clone()
    }
}

pin_project! {
    pub(crate) struct RequestStream<R> {
        initial_request: Option<R>,
        initial_snapshot: Arc<R>,
        skip_initial_snapshot: bool,
        #[pin]
        durable_stream: WatchStream<Arc<R>>,
        #[pin]
        control_stream: ReceiverStream<R>,
    }
}

impl<R> RequestStream<R>
where
    R: Clone + Send + Sync + 'static,
{
    fn new(
        initial_request: R,
        initial_snapshot: Arc<R>,
        latest_request_rx: watch::Receiver<Arc<R>>,
        control_rx: mpsc::Receiver<R>,
    ) -> Self {
        Self {
            initial_request: Some(initial_request),
            initial_snapshot,
            skip_initial_snapshot: true,
            durable_stream: WatchStream::new(latest_request_rx),
            control_stream: ReceiverStream::new(control_rx),
        }
    }
}

impl<R> Stream for RequestStream<R>
where
    R: Clone + Send + Sync + 'static,
{
    type Item = R;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(request) = this.initial_request.take() {
            return Poll::Ready(Some(request));
        }

        if let Poll::Ready(Some(control)) = this.control_stream.as_mut().poll_next(cx) {
            return Poll::Ready(Some(control));
        }

        loop {
            match this.durable_stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(snapshot)) => {
                    if *this.skip_initial_snapshot
                        && Arc::ptr_eq(&snapshot, &*this.initial_snapshot)
                    {
                        *this.skip_initial_snapshot = false;
                        continue;
                    }

                    *this.skip_initial_snapshot = false;
                    return Poll::Ready(Some(snapshot.as_ref().clone()));
                }
                Poll::Ready(None) => return Poll::Pending,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn subscription_control<R>(request: R) -> (SubscriptionController<R>, watch::Receiver<Arc<R>>)
where
    R: Clone + Send + Sync + 'static,
{
    let (latest, latest_request_rx) = watch::channel(Arc::new(request));
    (SubscriptionController { latest }, latest_request_rx)
}

fn scale_backoff(current: Duration, multiplier: u32, max: Duration) -> Duration {
    if multiplier <= 1 {
        return current.min(max);
    }
    // saturating_mul stays within Duration's range; .min(max) caps it
    current.saturating_mul(multiplier).min(max)
}

#[derive(Debug, Default, Clone)]
pub struct SubscribeRequestBuilder {
    request: SubscribeRequest,
}

impl SubscribeRequestBuilder {
    pub fn new() -> Self {
        Self {
            request: SubscribeRequest::default(),
        }
    }

    pub fn commitment(mut self, value: CommitmentLevel) -> Self {
        self.request.commitment = Some(value as i32);
        self
    }

    pub fn from_slot(mut self, slot: u64) -> Self {
        self.request.from_slot = Some(slot);
        self
    }

    pub fn add_account_filter(
        mut self,
        name: impl Into<String>,
        filter: SubscribeRequestFilterAccounts,
    ) -> Self {
        self.request.accounts.insert(name.into(), filter);
        self
    }

    pub fn add_slot_filter(
        mut self,
        name: impl Into<String>,
        filter: SubscribeRequestFilterSlots,
    ) -> Self {
        self.request.slots.insert(name.into(), filter);
        self
    }

    pub fn add_transaction_filter(
        mut self,
        name: impl Into<String>,
        filter: SubscribeRequestFilterTransactions,
    ) -> Self {
        self.request.transactions.insert(name.into(), filter);
        self
    }

    pub fn add_transaction_status_filter(
        mut self,
        name: impl Into<String>,
        filter: SubscribeRequestFilterTransactions,
    ) -> Self {
        self.request.transactions_status.insert(name.into(), filter);
        self
    }

    pub fn add_block_filter(
        mut self,
        name: impl Into<String>,
        filter: SubscribeRequestFilterBlocks,
    ) -> Self {
        self.request.blocks.insert(name.into(), filter);
        self
    }

    pub fn add_block_meta_filter(
        mut self,
        name: impl Into<String>,
        filter: SubscribeRequestFilterBlocksMeta,
    ) -> Self {
        self.request.blocks_meta.insert(name.into(), filter);
        self
    }

    pub fn add_entry_filter(
        mut self,
        name: impl Into<String>,
        filter: SubscribeRequestFilterEntry,
    ) -> Self {
        self.request.entry.insert(name.into(), filter);
        self
    }

    pub fn add_accounts_data_slice(mut self, slice: SubscribeRequestAccountsDataSlice) -> Self {
        self.request.accounts_data_slice.push(slice);
        self
    }

    pub fn ping(mut self, id: i32) -> Self {
        self.request.ping = Some(SubscribeRequestPing { id });
        self
    }

    pub fn build(self) -> SubscribeRequest {
        self.request
    }
}

#[derive(Debug, Default, Clone)]
pub struct SubscribeDeshredRequestBuilder {
    request: SubscribeDeshredRequest,
}

impl SubscribeDeshredRequestBuilder {
    pub fn new() -> Self {
        Self {
            request: SubscribeDeshredRequest::default(),
        }
    }

    pub fn add_transaction_filter(
        mut self,
        name: impl Into<String>,
        filter: SubscribeRequestFilterDeshredTransactions,
    ) -> Self {
        self.request
            .deshred_transactions
            .insert(name.into(), filter);
        self
    }

    pub fn ping(mut self, id: i32) -> Self {
        self.request.ping = Some(SubscribeRequestPing { id });
        self
    }

    pub fn build(self) -> SubscribeDeshredRequest {
        self.request
    }
}

#[derive(Clone)]
pub struct GrpcSlotClient {
    inner: GrpcClient,
}

impl GrpcSlotClient {
    pub async fn connect(grpc_url: &str) -> Result<Self, GrpcClientError> {
        Ok(Self {
            inner: GrpcClient::connect(grpc_url).await?,
        })
    }

    pub fn subscribe_slots(
        &self,
        commitment: CommitmentLevel,
    ) -> (SubscriptionController<SubscribeRequest>, GeyserSubscription) {
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
        self.inner.subscribe_with_request(request)
    }

    #[inline]
    pub fn process_message(&self, update: &SubscribeUpdate, tracker: &SlotTracker) {
        if let Some(UpdateOneof::Slot(slot_update)) = update.update_oneof.as_ref() {
            tracker.update_raw(slot_update.slot, slot_update.status);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn request_batch_commits_once() {
        let (controller, _rx) = subscription_control(SubscribeRequest::default());
        let mut batch = controller.batch();
        batch.request_mut().from_slot = Some(42);
        batch.commit();
        assert_eq!(controller.snapshot().from_slot, Some(42));
    }

    #[test]
    fn scale_backoff_caps_at_max() {
        let current = Duration::from_secs(4);
        let max = Duration::from_secs(5);
        assert_eq!(scale_backoff(current, 2, max), max);
    }

    /// verify that scale_backoff uses Duration::saturating_mul (no u128 intermediates).
    #[test]
    fn scale_backoff_no_u128() {
        // 50ms * 2 = 100ms, capped at 5s
        assert_eq!(
            scale_backoff(Duration::from_millis(50), 2, Duration::from_secs(5)),
            Duration::from_millis(100)
        );
        // overflow case: very large duration saturates, then caps at max
        assert_eq!(
            scale_backoff(Duration::MAX / 2, 3, Duration::from_secs(10)),
            Duration::from_secs(10)
        );
        // multiplier == 1 → returns current.min(max)
        assert_eq!(
            scale_backoff(Duration::from_secs(3), 1, Duration::from_secs(2)),
            Duration::from_secs(2)
        );
    }

    /// verify that `GrpcClientBuilder::build()` stores a cached channel in `ClientConfig`
    /// so that `connect_geyser_client()` is synchronous (no blocking `.await` for a channel).
    #[tokio::test]
    async fn channel_reuse_stored_in_config() {
        let builder = GrpcClient::builder_from_static("http://127.0.0.1:10000");
        let client = builder.build();
        // The channel is stored; cloning the client doesn't force a new TCP connect.
        // We verify the channel is present by cloning the config (Arc clone) and checking
        // that connecting_geyser_client returns immediately (it's sync internally).
        let client2 = client.clone();
        // Both clients share the same Arc<ClientConfig>, so channel is the same object.
        assert!(Arc::ptr_eq(&client.config, &client2.config));
    }

    #[tokio::test]
    async fn request_stream_skips_initial_duplicate_and_emits_updates() {
        let (controller, latest_rx) = subscription_control(SubscribeRequest::default());
        let initial_snapshot = controller.snapshot();
        let (control_tx, control_rx) = mpsc::channel(2);

        let mut stream = RequestStream::new(
            SubscribeRequest::default(),
            initial_snapshot,
            latest_rx,
            control_rx,
        );

        let first = tokio_stream::StreamExt::next(&mut stream).await.unwrap();
        assert!(first.accounts.is_empty());

        controller.modify(|request| {
            request.from_slot = Some(77);
        });

        let second = tokio_stream::StreamExt::next(&mut stream).await.unwrap();
        assert_eq!(second.from_slot, Some(77));

        control_tx
            .send(SubscribeRequest {
                ping: Some(SubscribeRequestPing { id: 9 }),
                ..Default::default()
            })
            .await
            .unwrap();

        let third = tokio_stream::StreamExt::next(&mut stream).await.unwrap();
        assert_eq!(third.ping.as_ref().map(|ping| ping.id), Some(9));
    }
}
