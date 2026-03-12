//! slot status subscriber with out-of-order detection.
//!
//! Yellowstone-gRPC does not reorder slot status events - it streams them
//! immediately as received from the validators geyser plugin. different
//! validator subsystems (turbine, banking stage) report independently, so
//! CREATED_BANK can arrive before FIRST_SHRED for the same slot.
//!
//! the sequential message IDs and `confirmed_at`/`finalized_at` fields in
//! yellowstone-grpc are for commitment-level filtering, not status reordering.
//!
//! this implementation detects OOO events via per-slot ring buffer tracking,
//! flags them with [OOO], but emits immediately without buffering for zero latency.
//!
//!
//!
// grpc:
// let message = Message::Slot(MessageSlot::from_geyser(...));  // allocation
//   inner.send_message(message);  // channel send
//   messages.entry(slot).or_default();  // BTreeMap lookup
//   slot_messages.messages_slots.push((msgid, message));  // vec push, clone
//   broadcast_tx.send(...);  // tokio broadcast

//   grpc-client hot path:
//   // atomics:
//   let entry = &self.ring[slot & 255];  // O(1) index
//   entry.last_order.fetch_max(order, Relaxed);  // 1 atomic
//   atomic.fetch_max(slot, Release);  // 1 atomic
//   self.update_seq.fetch_add(1, Release);  // 1 atomic

use grpc_client::{CommitmentLevel, GrpcSlotSubscriber, SlotStatus, SlotTracker};
use std::sync::Arc;
use std::time::Duration;

fn status_str(status: SlotStatus) -> &'static str {
    match status {
        SlotStatus::SlotFirstShredReceived => "FIRST_SHRED",
        SlotStatus::SlotCreatedBank => "CREATED_BANK",
        SlotStatus::SlotCompleted => "COMPLETED",
        SlotStatus::SlotProcessed => "PROCESSED",
        SlotStatus::SlotConfirmed => "CONFIRMED",
        SlotStatus::SlotFinalized => "FINALIZED",
        SlotStatus::SlotDead => "DEAD",
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let grpc_url = if args.len() > 1 {
        args[1].clone()
    } else {
        "http://localhost:10000".to_string()
    };

    let commitment = if args.len() > 2 {
        match args[2].to_lowercase().as_str() {
            "processed" => CommitmentLevel::Processed,
            "confirmed" => CommitmentLevel::Confirmed,
            "finalized" => CommitmentLevel::Finalized,
            _ => {
                eprintln!("invalid commitment level. use: processed, confirmed, or finalized");
                return;
            }
        }
    } else {
        CommitmentLevel::Processed
    };

    let cpu_core = if args.len() > 3 {
        args[3].parse().ok()
    } else {
        None
    };

    println!("connecting to: {}", grpc_url);
    println!("commitment level: {:?}", commitment);
    if let Some(core) = cpu_core {
        println!("pin to core: {}", core);
    }

    let tracker = Arc::new(SlotTracker::new());
    let mut subscriber =
        GrpcSlotSubscriber::new(grpc_url, Arc::clone(&tracker), commitment, cpu_core);

    subscriber.start();

    println!("waiting for connection...");
    if !subscriber.wait_for_connection(Duration::from_secs(10)) {
        eprintln!("failed to connect within timeout");
        return;
    }

    println!("connected! monitoring slot updates...");
    println!("press ctrl+c to exit\n");

    let mut last_seq = 0u64;

    loop {
        let seq = tracker.load_seq();

        if seq != last_seq {
            let (slot, status, ooo) = tracker.load_last_event();
            let ooo_flag = if ooo { " [OOO]" } else { "" };
            println!(
                "[{}] slot={} status={}{}",
                seq,
                slot,
                status_str(status),
                ooo_flag
            );
            last_seq = seq;
        }

        std::hint::spin_loop();
    }
}
