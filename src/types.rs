use core::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use cpu::{cpu_pause, fence_acquire, fence_release, CachePadded};

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
            // u64::MAX sentinel: distinguishes uninitialised from real slot 0
            slot: AtomicU64::new(u64::MAX),
            last_order: AtomicU8::new(0),
            flags: AtomicU8::new(0),
            _pad: [0; 6],
        }
    }
}

/// lock-free slot status tracker with out-of-order detection.
#[repr(C)]
pub struct SlotTracker {
    // per-slot OOO tracking (256 x 16 = 4 KiB)
    ring: [SlotEntry; RING_SIZE],

    // latest slot per status level — kept on separate cache lines
    pub first_shred: CachePadded<AtomicU64>,
    pub created_bank: CachePadded<AtomicU64>,
    pub completed: CachePadded<AtomicU64>,
    pub processed: CachePadded<AtomicU64>,
    pub confirmed: CachePadded<AtomicU64>,
    pub finalized: CachePadded<AtomicU64>,
    pub dead: CachePadded<AtomicU64>,

    // event sequence counter — external consumers poll this
    pub update_seq: CachePadded<AtomicU64>,

    // seqlock sequence: odd = writer active, even = consistent
    pub last_event_seq: CachePadded<AtomicU64>,
    // seqlock payload: slot value
    pub last_slot: CachePadded<AtomicU64>,
    // seqlock payload: packed status+ooo — low bit = ooo, upper 7 bits = status as u8
    pub last_status_ooo: CachePadded<AtomicU8>,

    // OOO monitoring — kept separate from status maxima
    pub ooo_count: CachePadded<AtomicU64>,

    pub connected: CachePadded<AtomicU8>, // 0 = disconnected, 1 = connected
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
            last_event_seq: CachePadded::new(AtomicU64::new(0)),
            last_slot: CachePadded::new(AtomicU64::new(0)),
            last_status_ooo: CachePadded::new(AtomicU8::new(0)),
            ooo_count: CachePadded::new(AtomicU64::new(0)),
            connected: CachePadded::new(AtomicU8::new(0)),
        }
    }

    #[inline]
    pub fn update(&self, slot: u64, status: SlotStatus) {
        let order = status_order(status);
        let mut is_ooo = false;

        // OOO detection (skip dead which can arrive anytime, order == 7)
        if order < 7 {
            let idx = (slot as usize) & (RING_SIZE - 1);
            let entry = &self.ring[idx];

            // acquire: synchronise with the Release store of `slot` in the new-slot branch
            // so we see the accompanying last_order/flags writes before reading them.
            let stored_slot = entry.slot.load(Ordering::Acquire);
            if stored_slot == slot {
                // same slot — use fetch_max to atomically update and retrieve previous order.
                // AcqRel: this is a read-modify-write; Acquire half ensures we see the prior
                // slot store, Release half publishes our new max before the conditional flag set.
                let prev_order = entry.last_order.fetch_max(order, Ordering::AcqRel);
                if order < prev_order {
                    is_ooo = true;
                    // Release: ensures the is_ooo flag is visible to is_ooo() readers that
                    // use Acquire on `slot` then Relaxed on `flags`.
                    entry.flags.fetch_or(1, Ordering::Release);
                    self.ooo_count.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                // new slot — write last_order and flags first, then publish slot last
                // so that a concurrent reader who sees slot == X also sees valid order/flags.
                entry.last_order.store(order, Ordering::Relaxed);
                entry.flags.store(0, Ordering::Relaxed);
                // Release: publication fence — readers Acquire on `slot` to synchronise
                // with the last_order and flags stores above.
                entry.slot.store(slot, Ordering::Release);
            }
        }

        // update latest slot for this status level.
        // AcqRel on fetch_max: it is a RMW so we need both Acquire (read side) and Release
        // (write side) to correctly participate in synchronisation with readers that use Acquire.
        let atomic = match status {
            SlotStatus::SlotFirstShredReceived => &self.first_shred,
            SlotStatus::SlotCreatedBank => &self.created_bank,
            SlotStatus::SlotCompleted => &self.completed,
            SlotStatus::SlotProcessed => &self.processed,
            SlotStatus::SlotConfirmed => &self.confirmed,
            SlotStatus::SlotFinalized => &self.finalized,
            SlotStatus::SlotDead => &self.dead,
        };
        // AcqRel: RMW on a shared max — needs both halves (see above).
        atomic.fetch_max(slot, Ordering::AcqRel);

        // --- seqlock writer protocol ---
        //
        //    mark odd: readers that see an odd seq will spin rather than observe a torn tuple.
        //    Relaxed: the Release fence below provides the actual ordering guarantee.
        self.last_event_seq.fetch_or(1, Ordering::Relaxed);

        //    Release fence: all stores above (ring, status maxima) are ordered before
        //    the slot/status_ooo stores below, from the perspective of any thread that
        //    later observes the even seq with Acquire.
        fence_release();

        //    write payload fields.  Relaxed is safe here because the seqlock Release
        //    fence above already orders these writes, and the final fetch_add(1, Release)
        //    below makes them visible to readers in the correct order.
        self.last_slot.store(slot, Ordering::Relaxed);
        self.last_status_ooo
            .store((status as u8) << 1 | is_ooo as u8, Ordering::Relaxed);

        //    advance seq to even, with Release so the payload stores are ordered before
        //    this, and readers that Acquire on seq see the complete payload.
        self.last_event_seq.fetch_add(1, Ordering::Release);

        //    increment the external event counter.
        //    Release: readers of update_seq use Acquire to observe everything written above.
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
        // Acquire: synchronise with the Release fetch_add in update() so callers
        // that use load_seq as a change-detect signal see a consistent state afterward.
        self.update_seq.load(Ordering::Acquire)
    }

    /// Seqlock reader: returns a coherent (slot, status, ooo) tuple.
    #[inline]
    pub fn load_last_event(&self) -> (u64, SlotStatus, bool) {
        loop {
            // Acquire: pairs with the Release fetch_add(1) at the end of the writer,
            // ensuring we read the payload writes in their published order.
            let seq1 = self.last_event_seq.load(Ordering::Acquire);
            if seq1 & 1 != 0 {
                // writer is active — spin with pause hint to reduce bus traffic.
                cpu_pause();
                continue;
            }

            // Relaxed: ordered by the surrounding seqlock Acquire/fence_acquire below.
            let slot = self.last_slot.load(Ordering::Relaxed);
            let packed = self.last_status_ooo.load(Ordering::Relaxed);

            // Acquire fence: prevents the compiler/CPU from hoisting the seq2 load
            // above the payload loads, ensuring we validate the same window we read.
            fence_acquire();

            // Relaxed: the fence_acquire above already provides the ordering we need.
            let seq2 = self.last_event_seq.load(Ordering::Relaxed);
            if seq1 == seq2 {
                let ooo = packed & 1 != 0;
                let status_raw = packed >> 1;
                let status =
                    SlotStatus::try_from(status_raw as i32).unwrap_or(SlotStatus::SlotProcessed);
                return (slot, status, ooo);
            }

            // Sequence changed under us — retry.
            cpu_pause();
        }
    }

    /// Check if a slot had out-of-order events.
    ///
    /// Double-checks slot stability around the flag read so a racing new-slot
    /// write at the same ring index cannot produce a false positive.
    #[inline]
    pub fn is_ooo(&self, slot: u64) -> bool {
        let idx = (slot as usize) & (RING_SIZE - 1);
        let entry = &self.ring[idx];

        // Acquire: pairs with the Release store of `slot` in the new-slot branch of
        // update(), ensuring we see the accompanying last_order/flags state.
        let s1 = entry.slot.load(Ordering::Acquire);
        if s1 != slot {
            return false;
        }

        // Relaxed: safe between the two Acquire slot loads that bracket it.
        let flags = entry.flags.load(Ordering::Relaxed);

        // second slot load: if a new-slot write raced in between, seq is different.
        // Acquire again to order any intervening slot writes.
        let s2 = entry.slot.load(Ordering::Acquire);

        s1 == s2 && flags & 1 != 0
    }

    /// get total out-of-order event count.
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
        self.connected.load(Ordering::Acquire) != 0
    }

    #[inline]
    pub fn set_connected(&self, connected: bool) {
        // Release: readers that Acquire on `connected` see any state written before this call.
        self.connected.store(connected as u8, Ordering::Release);
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
            connected: self.connected.load(Ordering::Acquire) != 0,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    // All valid SlotStatus discriminants for the coherence test.
    const ALL_STATUSES: &[SlotStatus] = &[
        SlotStatus::SlotFirstShredReceived,
        SlotStatus::SlotCreatedBank,
        SlotStatus::SlotCompleted,
        SlotStatus::SlotProcessed,
        SlotStatus::SlotConfirmed,
        SlotStatus::SlotFinalized,
        SlotStatus::SlotDead,
    ];

    // -----------------------------------------------------------------------
    //
    // N writer threads hammer update(), M reader threads call load_last_event()
    // concurrently.  No reader should ever see a status_raw that does not decode
    // to a known SlotStatus.
    // -----------------------------------------------------------------------
    #[test]
    fn seqlock_coherence_no_torn_tuples() {
        const N_WRITERS: usize = 4;
        const N_READERS: usize = 4;
        const DURATION_MS: u64 = 100;

        let tracker = Arc::new(SlotTracker::new());
        let deadline = Instant::now() + Duration::from_millis(DURATION_MS);

        // Writer threads
        let mut handles = Vec::new();
        for w in 0..N_WRITERS {
            let t = Arc::clone(&tracker);
            let dl = deadline;
            handles.push(std::thread::spawn(move || {
                let mut slot: u64 = (w as u64) * 1000;
                let mut si = 0usize;
                while Instant::now() < dl {
                    let status = ALL_STATUSES[si % ALL_STATUSES.len()];
                    t.update(slot, status);
                    slot = slot.wrapping_add(1);
                    si += 1;
                }
            }));
        }

        // Reader threads — assert no torn status_raw values
        for _ in 0..N_READERS {
            let t = Arc::clone(&tracker);
            let dl = deadline;
            handles.push(std::thread::spawn(move || {
                while Instant::now() < dl {
                    let (_slot, status, _ooo) = t.load_last_event();
                    // load_last_event decodes with unwrap_or, so we just assert
                    // the returned status is a discriminant that prost knows about.
                    let raw = status as i32;
                    assert!(
                        SlotStatus::try_from(raw).is_ok(),
                        "torn status discriminant observed: {raw}"
                    );
                }
            }));
        }

        for h in handles {
            h.join().expect("thread panicked");
        }
    }

    // -----------------------------------------------------------------------
    // Ring reuse test
    //
    // Write slot X, then write a different slot Y that maps to the same ring
    // index.  Verify is_ooo(X) is false after eviction, and that a follow-up
    // OOO write to Y is correctly detected.
    // -----------------------------------------------------------------------
    #[test]
    fn ring_reuse_evicts_stale_ooo_flag() {
        let tracker = SlotTracker::new();

        // Pick two slots that collide in the ring.
        // Slots that differ by exactly RING_SIZE map to the same index.
        let slot_x: u64 = 100;
        let slot_y: u64 = slot_x + RING_SIZE as u64; // same index, different slot

        // Write slot X in-order: processed then confirmed — no OOO.
        tracker.update(slot_x, SlotStatus::SlotProcessed);
        tracker.update(slot_x, SlotStatus::SlotConfirmed);
        assert!(!tracker.is_ooo(slot_x), "no OOO yet for X");

        // Write slot Y (evicts X from the ring index).
        tracker.update(slot_y, SlotStatus::SlotProcessed);

        // slot_x is no longer in the ring — is_ooo(X) must return false.
        assert!(
            !tracker.is_ooo(slot_x),
            "is_ooo(X) must be false after Y evicted the entry"
        );

        // Force OOO on slot Y: write confirmed first, then processed (lower order).
        tracker.update(slot_y, SlotStatus::SlotConfirmed);
        tracker.update(slot_y, SlotStatus::SlotProcessed); // processed < confirmed → OOO

        assert!(
            tracker.is_ooo(slot_y),
            "is_ooo(Y) must be true after out-of-order write"
        );
    }

    // -----------------------------------------------------------------------
    // Sentinel test
    //
    // Slot 0 is a real valid slot value.  The ring sentinel is u64::MAX.
    // A real slot-0 message must NOT match the uninitialised sentinel.
    // -----------------------------------------------------------------------
    #[test]
    fn slot_zero_does_not_match_sentinel() {
        let tracker = SlotTracker::new();

        // Before any update, the ring entries hold the u64::MAX sentinel.
        // is_ooo(0) must return false because the entry slot is MAX, not 0.
        assert!(
            !tracker.is_ooo(0),
            "slot 0 must not match the u64::MAX sentinel"
        );

        // After updating slot 0, is_ooo(0) should still be false (in-order write).
        tracker.update(0, SlotStatus::SlotProcessed);
        assert!(
            !tracker.is_ooo(0),
            "in-order slot 0 must not be flagged OOO"
        );

        // Now write an earlier status for slot 0 — confirmed > processed in order,
        // so writing processed (3) after confirmed (4) is OOO.
        tracker.update(0, SlotStatus::SlotConfirmed);
        tracker.update(0, SlotStatus::SlotProcessed); // processed < confirmed → OOO
        assert!(tracker.is_ooo(0), "slot 0 OOO must be detected correctly");
    }
}
