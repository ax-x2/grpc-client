pub mod client;
pub mod codec;
pub mod subscriber;
pub mod types;

#[allow(clippy::all)]
mod proto {
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

pub use subscriber::GrpcSlotSubscriber;
pub use types::{CommitmentLevel, SlotStatus, SlotTracker, SlotTrackerSnapshot};
