use core::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use cpu::CachePadded;

pub use crate::proto::geyser::{CommitmentLevel, SlotStatus};

const RING_SIZE: usize = 256;

/// logical order of slot statuses (lower = earlier in progression)
#[inline]
const fn status_order(status: SlotStatus) -> u8 {
    match status {
        SlotStatus::SlotFirstShredReceived => 0,
        SlotStatus::SlotCreatedBank => 1,
        SlotStatus::SlotCompleted => 2,
        SlotStatus::SlotProcessed => 3,
        SlotStatus::SlotConfirmed => 4,
        SlotStatus::SlotFinalized => 5,
        SlotStatus::SlotDead => 7, // dead can arrive anytime
    }
}

/// per-slot tracking entry for OOO detection (16 bytes)
#[repr(C)]
struct SlotEntry {
    slot: AtomicU64,
    last_order: AtomicU8,
    flags: AtomicU8,
    _pad: [u8; 6],
}

impl SlotEntry {
    const fn new() -> Self {
        Self {
            slot: AtomicU64::new(0),
            last_order: AtomicU8::new(0),
            flags: AtomicU8::new(0),
            _pad: [0; 6],
        }
    }
}

/// lock-free slot status tracker with out-of-order detection.
#[repr(C)]
pub struct SlotTracker {
    // per-slot OOO tracking (256 x 16 = 4KB)
    ring: [SlotEntry; RING_SIZE],

    // latest slot per status level
    pub first_shred: CachePadded<AtomicU64>,
    pub created_bank: CachePadded<AtomicU64>,
    pub completed: CachePadded<AtomicU64>,
    pub processed: CachePadded<AtomicU64>,
    pub confirmed: CachePadded<AtomicU64>,
    pub finalized: CachePadded<AtomicU64>,
    pub dead: CachePadded<AtomicU64>,

    // event tracking
    pub update_seq: CachePadded<AtomicU64>,
    pub last_slot: CachePadded<AtomicU64>,
    pub last_status: CachePadded<AtomicU8>,
    pub last_ooo: CachePadded<AtomicBool>,

    // OOO monitoring
    pub ooo_count: CachePadded<AtomicU64>,

    pub connected: CachePadded<AtomicBool>,
}

// manual debug impl since arrays > 32 dont auto-derive
impl core::fmt::Debug for SlotTracker {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SlotTracker")
            .field("first_shred", &self.first_shred.load(Ordering::Relaxed))
            .field("processed", &self.processed.load(Ordering::Relaxed))
            .field("confirmed", &self.confirmed.load(Ordering::Relaxed))
            .field("finalized", &self.finalized.load(Ordering::Relaxed))
            .field("ooo_count", &self.ooo_count.load(Ordering::Relaxed))
            .finish()
    }
}

impl SlotTracker {
    pub const fn new() -> Self {
        // const array init
        const ENTRY: SlotEntry = SlotEntry::new();
        Self {
            ring: [ENTRY; RING_SIZE],
            first_shred: CachePadded::new(AtomicU64::new(0)),
            created_bank: CachePadded::new(AtomicU64::new(0)),
            completed: CachePadded::new(AtomicU64::new(0)),
            processed: CachePadded::new(AtomicU64::new(0)),
            confirmed: CachePadded::new(AtomicU64::new(0)),
            finalized: CachePadded::new(AtomicU64::new(0)),
            dead: CachePadded::new(AtomicU64::new(0)),
            update_seq: CachePadded::new(AtomicU64::new(0)),
            last_slot: CachePadded::new(AtomicU64::new(0)),
            last_status: CachePadded::new(AtomicU8::new(0)),
            last_ooo: CachePadded::new(AtomicBool::new(false)),
            ooo_count: CachePadded::new(AtomicU64::new(0)),
            connected: CachePadded::new(AtomicBool::new(false)),
        }
    }

    #[inline]
    pub fn update(&self, slot: u64, status: SlotStatus) {
        let order = status_order(status);
        let mut is_ooo = false;

        // OOO detection (skip dead which can arrive anytime)
        if order < 7 {
            let idx = (slot as usize) & (RING_SIZE - 1);
            let entry = &self.ring[idx];

            let stored_slot = entry.slot.load(Ordering::Acquire);
            if stored_slot == slot {
                // same slot - check order
                let last = entry.last_order.load(Ordering::Acquire);
                if order < last {
                    // out of order detected
                    is_ooo = true;
                    entry.flags.fetch_or(1, Ordering::Relaxed);
                    self.ooo_count.fetch_add(1, Ordering::Relaxed);
                }
                entry.last_order.fetch_max(order, Ordering::Relaxed);
            } else {
                // new slot - initialize
                entry.slot.store(slot, Ordering::Relaxed);
                entry.last_order.store(order, Ordering::Relaxed);
                entry.flags.store(0, Ordering::Relaxed);
            }
        }

        // update latest slot for this status level
        let atomic = match status {
            SlotStatus::SlotFirstShredReceived => &self.first_shred,
            SlotStatus::SlotCreatedBank => &self.created_bank,
            SlotStatus::SlotCompleted => &self.completed,
            SlotStatus::SlotProcessed => &self.processed,
            SlotStatus::SlotConfirmed => &self.confirmed,
            SlotStatus::SlotFinalized => &self.finalized,
            SlotStatus::SlotDead => &self.dead,
        };
        atomic.fetch_max(slot, Ordering::Release);

        // track last event
        self.last_slot.store(slot, Ordering::Release);
        self.last_status.store(status as u8, Ordering::Release);
        self.last_ooo.store(is_ooo, Ordering::Release);
        self.update_seq.fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub fn update_raw(&self, slot: u64, status: i32) {
        if let Ok(s) = SlotStatus::try_from(status) {
            self.update(slot, s);
        }
    }

    #[inline]
    pub fn load_seq(&self) -> u64 {
        self.update_seq.load(Ordering::Acquire)
    }

    #[inline]
    pub fn load_last_event(&self) -> (u64, SlotStatus, bool) {
        let slot = self.last_slot.load(Ordering::Acquire);
        let status_raw = self.last_status.load(Ordering::Acquire);
        let ooo = self.last_ooo.load(Ordering::Acquire);
        let status = SlotStatus::try_from(status_raw as i32).unwrap_or(SlotStatus::SlotProcessed);
        (slot, status, ooo)
    }

    /// check if a slot had out-of-order events
    #[inline]
    pub fn is_ooo(&self, slot: u64) -> bool {
        let idx = (slot as usize) & (RING_SIZE - 1);
        let entry = &self.ring[idx];
        entry.slot.load(Ordering::Acquire) == slot && entry.flags.load(Ordering::Acquire) & 1 != 0
    }

    /// get total out-of-order event count
    #[inline]
    pub fn ooo_count(&self) -> u64 {
        self.ooo_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn load_first_shred(&self) -> u64 {
        self.first_shred.load(Ordering::Acquire)
    }

    #[inline]
    pub fn load_created_bank(&self) -> u64 {
        self.created_bank.load(Ordering::Acquire)
    }

    #[inline]
    pub fn load_completed(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn load_processed(&self) -> u64 {
        self.processed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn load_confirmed(&self) -> u64 {
        self.confirmed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn load_finalized(&self) -> u64 {
        self.finalized.load(Ordering::Acquire)
    }

    #[inline]
    pub fn load_dead(&self) -> u64 {
        self.dead.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_processed(&self, slot: u64) -> bool {
        slot <= self.processed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_confirmed(&self, slot: u64) -> bool {
        slot <= self.confirmed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_finalized(&self, slot: u64) -> bool {
        slot <= self.finalized.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Release);
    }

    #[inline]
    pub fn snapshot(&self) -> SlotTrackerSnapshot {
        SlotTrackerSnapshot {
            first_shred: self.first_shred.load(Ordering::Acquire),
            created_bank: self.created_bank.load(Ordering::Acquire),
            completed: self.completed.load(Ordering::Acquire),
            processed: self.processed.load(Ordering::Acquire),
            confirmed: self.confirmed.load(Ordering::Acquire),
            finalized: self.finalized.load(Ordering::Acquire),
            dead: self.dead.load(Ordering::Acquire),
            connected: self.connected.load(Ordering::Acquire),
        }
    }
}

impl Default for SlotTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SlotTrackerSnapshot {
    pub first_shred: u64,
    pub created_bank: u64,
    pub completed: u64,
    pub processed: u64,
    pub confirmed: u64,
    pub finalized: u64,
    pub dead: u64,
    pub connected: bool,
}
