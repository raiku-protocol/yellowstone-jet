//! A Yellowstone-specific UpcomingLeaderPredictor implementation
//!
//! This module provides an implementation of the UpcomingLeaderPredictor trait
//! tailored for Yellowstone, utilizing gRPC and RPC services to track the current slot
//! and predict upcoming leaders.
//!
//! # Safety
//!
//! This module is designed to be thread-safe and can be shared across multiple tasks.
//!
//! # Poisoning
//!
//! The managed schedule used in this implementation can be poisoned if the background task
//! updating it panics or is dropped. The slot tracker is disconnected while its stream
//! reconnects; prediction yields no leaders then.
//!
use {
    crate::{
        core::UpcomingLeaderPredictor, rpc::schedule::ManagedLeaderSchedule, slot::SlotTracker,
    },
    solana_pubkey::Pubkey,
};

///
/// A Yellowstone-specific implementation of UpcomingLeaderPredictor
///
/// # Safety
///
/// This struct is cheaply-cloneable and can be shared between threads.
///
#[derive(Clone)]
pub struct YellowstoneUpcomingLeader {
    pub slot_tracker: SlotTracker,
    pub managed_schedule: ManagedLeaderSchedule,
}

impl UpcomingLeaderPredictor for YellowstoneUpcomingLeader {
    fn try_predict_next_n_leaders(&self, n: usize) -> Vec<Pubkey> {
        let Ok(slot) = self.slot_tracker.load() else {
            // No live tip to predict from while the slot stream reconnects; skip
            // pre-connecting this round rather than guess from a stale slot.
            return Vec::new();
        };
        let reminder = slot % 4;

        let next_leader_boundary = slot + (4 - reminder);
        (0..n)
            .map(|i| next_leader_boundary + (i * 4) as u64)
            .flat_map(|s| self.managed_schedule.get_leader(s).expect("get_leader"))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::sync::atomic::Ordering};

    #[test]
    fn predicts_nothing_while_slot_tracker_disconnected() {
        let leaders = vec![
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        let predictor = YellowstoneUpcomingLeader {
            slot_tracker: SlotTracker::new(1),
            managed_schedule: ManagedLeaderSchedule::new_for_test(0, leaders.clone()),
        };
        assert_eq!(predictor.try_predict_next_n_leaders(2), leaders[1..3]);

        predictor
            .slot_tracker
            .inner
            .closed
            .store(true, Ordering::Release);
        assert!(predictor.try_predict_next_n_leaders(2).is_empty());
    }
}
