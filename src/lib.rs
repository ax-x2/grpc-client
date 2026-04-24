pub mod benchmark;
pub mod client;
pub mod codec;
pub mod raw;
pub mod subscriber;
pub mod types;

#[allow(clippy::all)]
pub mod proto {
    pub mod solana {
        pub mod storage {
            pub mod confirmed_block {
                include!("proto/solana.storage.confirmed_block.rs");
            }
        }
    }

    pub mod geyser {
        include!("proto/geyser.rs");
    }
}

pub use benchmark::{
    BenchmarkConfig, BenchmarkEndpoint, BenchmarkHandle, BenchmarkSnapshot, EndpointScore,
    spawn_deshred_benchmark, spawn_subscribe_benchmark,
};
pub use client::{
    DeshredSubscription, GeyserSubscription, GrpcClient, GrpcClientBuilder, GrpcClientBuilderError,
    GrpcClientError, GrpcSlotClient, ReconnectConfig, RequestBatch, SubscribeDeshredRequestBuilder,
    SubscribeRequestBuilder, SubscriptionConfig, SubscriptionController, SubscriptionError,
};
pub use raw::{
    AccountKeyIter, BlockView, RawSlotUpdate, RawSubscribeFrame, RawSubscription, TokenBalanceIter,
    TokenBalanceView, TxIter, TxView, parse_block_update_from_bytes,
};
pub use subscriber::GrpcSlotSubscriber;
pub use types::{CommitmentLevel, SlotStatus, SlotTracker, SlotTrackerSnapshot};
