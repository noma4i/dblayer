//! The calls of the platform to the owner Transfer and to Rounds: each registers a waiter, posts one input and returns the waiter.

use std::sync::atomic::Ordering;

use super::{Life, LifeReply};
use crate::owners::write::outbox::{Fault, Receipt};
use crate::owners::write::rounds::RoundsInput;
use crate::owners::write::transfer::{BatchItem, Joint, SuppliedSource, TransferAnswer, TransferInput};
use crate::runtime::{Epoch, EpochStamp, Time, Waiter};

impl Life {
    fn transfer_call(&self, epoch: Epoch, input: impl FnOnce(u64) -> TransferInput) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        if self.wiring.addresses().is_none_or(|addresses| addresses.transfer.is_none()) { return self.settled(LifeReply::Transfer(TransferAnswer::Err(Fault::NoSession))); }
        let waiter = self.wiring.outcomes.register(epoch);
        self.wiring.to_transfer_at(EpochStamp::Epoch(epoch), input(waiter.id().raw()));
        waiter
    }

    pub fn accept_upload(&self, epoch: Epoch, item: BatchItem, now: Time) -> Waiter<LifeReply> { self.transfer_call(epoch, |reply| TransferInput::Accept { epoch, item: Box::new(item), reply, now }) }

    pub fn accept_upload_batch(&self, epoch: Epoch, items: Vec<BatchItem>, joint: Joint, now: Time) -> Waiter<LifeReply> { self.transfer_call(epoch, |reply| TransferInput::AcceptBatch { epoch, items, joint: Box::new(joint), reply, now }) }

    pub fn cancel_upload(&self, epoch: Epoch, key: String) -> Waiter<LifeReply> { self.transfer_call(epoch, |reply| TransferInput::Cancel { key, reply }) }

    pub fn retry_upload(&self, epoch: Epoch, key: String, source: Option<SuppliedSource>) -> Waiter<LifeReply> { self.transfer_call(epoch, |reply| TransferInput::Retry { key, source, reply }) }

    pub fn supply_source(&self, epoch: Epoch, key: String, source: SuppliedSource) -> Waiter<LifeReply> { self.transfer_call(epoch, |reply| TransferInput::SupplySource { epoch, key, source, reply }) }

    pub fn fail_source(&self, epoch: Epoch, key: String, message: String) -> Waiter<LifeReply> { self.transfer_call(epoch, |reply| TransferInput::SourceFailed { epoch, key, message, reply }) }

    pub fn discard_upload(&self, epoch: Epoch, key: String) -> Waiter<LifeReply> { self.transfer_call(epoch, |reply| TransferInput::Discard { key, reply }) }

    pub fn upload_outcome(&self, epoch: Epoch, receipt: Receipt) -> Waiter<LifeReply> { self.transfer_call(epoch, |reply| TransferInput::Outcome { receipt, reply }) }

    pub fn jobs_sink_set(&self) { self.wiring.to_rounds(RoundsInput::JobsSinkSet); }

    pub fn has_rounds(&self) -> bool { self.wiring.addresses().is_some_and(|addresses| addresses.rounds.is_some()) }

    pub fn round_started(&self, request_id: u64, round_id: u32, epoch: Epoch) { self.wiring.to_rounds(RoundsInput::RoundStarted { request_id, round_id, epoch }); }

    pub fn round_refused(&self, request_id: u64, epoch: Epoch, reason: String) { self.wiring.to_rounds(RoundsInput::RoundRefused { request_id, epoch, reason }); }

    pub fn round_ended(&self, round_id: u32, epoch: Epoch) { self.wiring.to_rounds(RoundsInput::RoundEnded { round_id, epoch }); }
}
