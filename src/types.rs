use crate::config::Config;
use crate::pending::PendingRequests;
use crate::state::NodeState;
use crate::verified::TrustedNetworkState;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::{Mutex, Semaphore};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct TickStatus {
    pub(crate) epoch: u16,
    pub(crate) tick: u32,
    pub(crate) initial_tick: u32,
    pub(crate) tick_duration_ms: u16,
    pub(crate) aligned_votes: u16,
    pub(crate) misaligned_votes: u16,
}

pub(crate) fn pack_epoch_tick(epoch: u16, tick: u32) -> u64 {
    ((epoch as u64) << 32) | (tick as u64)
}

pub(crate) fn unpack_epoch_tick(packed: u64) -> Option<(u16, u32)> {
    if packed == 0 {
        None
    } else {
        Some(((packed >> 32) as u16, packed as u32))
    }
}

pub(crate) fn format_epoch_tick_packed(packed: u64) -> String {
    match unpack_epoch_tick(packed) {
        Some((epoch, tick)) => format!("epoch={epoch} tick={tick}"),
        None => "epoch=? tick=?".to_string(),
    }
}

#[derive(Clone)]
pub(crate) struct ApiState {
    pub(crate) node_state: Arc<Mutex<NodeState>>,
    pub(crate) pending_requests: Arc<PendingRequests>,
    pub(crate) latest_epoch_tick: Arc<AtomicU64>,
    pub(crate) trusted_network: Arc<TrustedNetworkState>,
    pub(crate) outbound_budget: Arc<Semaphore>,
    pub(crate) config: Arc<Config>,
}
