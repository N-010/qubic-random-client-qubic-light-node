use crate::config::Config;
use crate::state::NodeState;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug, Serialize)]
pub(crate) struct TickStatus {
    pub(crate) epoch: u16,
    pub(crate) tick: u32,
    pub(crate) initial_tick: u32,
    pub(crate) tick_duration_ms: u16,
    pub(crate) aligned_votes: u16,
    pub(crate) misaligned_votes: u16,
}

pub(crate) fn format_epoch_tick(status: Option<TickStatus>) -> String {
    match status {
        Some(status) => format!("epoch={} tick={}", status.epoch, status.tick),
        None => "epoch=? tick=?".to_string(),
    }
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

pub(crate) fn tick_status_from_packed(packed: u64) -> Option<TickStatus> {
    unpack_epoch_tick(packed).map(|(epoch, tick)| TickStatus {
        epoch,
        tick,
        initial_tick: 0,
        tick_duration_ms: 0,
        aligned_votes: 0,
        misaligned_votes: 0,
    })
}

#[derive(Clone)]
pub(crate) struct ApiState {
    pub(crate) node_state: Arc<Mutex<NodeState>>,
    pub(crate) latest_epoch_tick: Arc<AtomicU64>,
    pub(crate) config: Arc<Config>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BalanceResponse {
    pub(crate) wallet: String,
    pub(crate) public_key_hex: String,
    pub(crate) tick: u32,
    pub(crate) spectrum_index: i32,
    pub(crate) incoming_amount: i64,
    pub(crate) outgoing_amount: i64,
    pub(crate) balance: i64,
    pub(crate) number_of_incoming_transfers: u32,
    pub(crate) number_of_outgoing_transfers: u32,
    pub(crate) latest_incoming_transfer_tick: u32,
    pub(crate) latest_outgoing_transfer_tick: u32,
}

#[derive(Debug, Serialize)]
pub(crate) struct TickTransaction {
    pub(crate) source_public_key_hex: String,
    pub(crate) destination_public_key_hex: String,
    pub(crate) amount: i64,
    pub(crate) tick: u32,
    pub(crate) input_type: u16,
    pub(crate) input_size: u16,
    pub(crate) input_hex: String,
    pub(crate) signature_hex: String,
}
