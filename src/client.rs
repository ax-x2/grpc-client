use crate::proto::geyser::{
    geyser_client::GeyserClient, subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterSlots, SubscribeRequestPing, SubscribeUpdate,
};
use crate::types::SlotTracker;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

pub struct GrpcSlotClient {
    client: GeyserClient<Channel>,
    // ping_counter: i32,
    last_ping: Instant,
}

impl GrpcSlotClient {
    pub async fn connect(grpc_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let channel = Channel::from_shared(grpc_url.to_string())?
            .connect()
            .await?;

        let client = GeyserClient::new(channel);

        Ok(Self {
            client,
            // ping_counter: 0,
            last_ping: Instant::now(),
        })
    }

    pub async fn subscribe_slots(
        &mut self,
        commitment: CommitmentLevel,
    ) -> Result<Streaming<SubscribeUpdate>, Box<dyn std::error::Error>> {
        let mut slots_filter = HashMap::with_capacity(1);
        slots_filter.insert(
            "client".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            },
        );

        let subscribe_request = SubscribeRequest {
            slots: slots_filter,
            commitment: Some(commitment as i32),
            ..Default::default()
        };

        let request_stream = tokio_stream::once(subscribe_request);

        let response = self.client.subscribe(Request::new(request_stream)).await?;

        Ok(response.into_inner())
    }

    pub fn should_send_ping(&self) -> bool {
        self.last_ping.elapsed() > Duration::from_secs(5)
    }

    #[inline]
    pub fn process_message(&self, update: SubscribeUpdate, tracker: &SlotTracker) {
        if let Some(update_oneof) = update.update_oneof {
            match update_oneof {
                UpdateOneof::Slot(slot_update) => {
                    tracker.update_raw(slot_update.slot, slot_update.status);
                }
                UpdateOneof::Pong(_) => {}
                _ => {}
            }
        }
    }
}
