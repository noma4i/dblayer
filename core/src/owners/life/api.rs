//! The calls of the platform to the owners: each registers a waiter, posts one input and returns the waiter.

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

use super::{Life, LifeReply, WAITER_AUTHORS};
use crate::owners::calls::{CallKind, CallsInput};
use crate::owners::contract::{BatchAuthor, BatchId, RequestId};
use crate::owners::store::StoreReply;
use crate::owners::lifecycle::LifecycleInput;
use crate::owners::network::NetworkInput;
use crate::owners::session::{AuthorId, AuthorOutcome, SessionInput};
use crate::owners::write::outbox::{Fault, OutboxAnswer, OutboxInput};
use crate::runtime::{Epoch, EpochStamp, Waiter};

impl Life {
    /// `set_online`: Network answers after its `step`.
    pub fn set_online(&self, online: bool) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_network(NetworkInput::SetOnline { online, reply: waiter.id().raw() });
        waiter
    }

    /// `foreground`: Network answers after its `step`.
    pub fn foreground(&self) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_network(NetworkInput::Foreground { reply: waiter.id().raw() });
        waiter
    }

    /// `set_power_saving`: Network answers after its `step`.
    pub fn set_power_saving(&self, saving: bool) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_network(NetworkInput::PowerSaving { saving, reply: waiter.id().raw() });
        waiter
    }

    /// The rule of a call of account data for the epoch of the caller.
    pub(crate) fn verdict(&self, epoch: Epoch) -> super::call_rule::CallVerdict {
        if self.shut.load(Ordering::SeqCst) { return super::call_rule::CallVerdict::Closed; }
        let published = self.published();
        match super::call_rule::call_verdict(&published, epoch, self.with_session) {
            super::call_rule::CallVerdict::Pass if published.epoch != epoch => super::call_rule::CallVerdict::Refuse(crate::owners::calls::CallError::NoSession),
            verdict => verdict,
        }
    }

    pub fn graphql_path(&self) -> Option<&str> { self.graphql_path.as_deref() }

    /// `run`: the platform call rule, then the owner Calls.
    pub fn run(&self, epoch: Epoch, operation: String, variables: String) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        let published = self.published();
        match super::call_rule::call_verdict(&published, epoch, self.with_session) {
            super::call_rule::CallVerdict::Closed => return self.settled(LifeReply::Closed),
            super::call_rule::CallVerdict::Refuse(error) => return self.settled(LifeReply::Call(Err(error))),
            super::call_rule::CallVerdict::Pass => {}
        }
        self.call(epoch, operation, variables)
    }

    fn call(&self, epoch: Epoch, operation: String, variables: String) -> Waiter<LifeReply> {
        let waiter = self.wiring.outcomes.register(epoch);
        self.wiring.to_calls(CallsInput::Run { epoch, operation, variables, reply: waiter.id().raw() });
        waiter
    }

    /// `session_call`: the owner Session, the answer by `Outcomes`.
    pub fn session_call(&self, operation: String, body: String) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        let published = self.published();
        if published.closed {
            return self.settled(LifeReply::Closed);
        }
        if self.wiring.addresses().is_none_or(|addresses| addresses.session.is_none()) {
            return self.settled(LifeReply::Session(Err(crate::owners::session::SessionError::Invalid)));
        }
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_session(SessionInput::SessionCall { operation, body, reply: waiter.id().raw() });
        waiter
    }

    pub fn interrupted_session_answer(&self) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) || self.published().closed { return self.settled(LifeReply::Closed); }
        if self.wiring.addresses().is_none_or(|addresses| addresses.session.is_none()) {
            return self.settled(LifeReply::SessionResume(Err(crate::owners::session::SessionError::NoSession)));
        }
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_session(SessionInput::SessionResume { reply: waiter.id().raw() });
        waiter
    }

    /// `Mutate` of Outbox and Transfer: the answer comes to the sink the author registered with `on_mutate_answer`.
    pub fn mutate(&self, author: AuthorId, owner: Option<crate::owners::session::MutateOwner>, req_id: RequestId, path: String, body: String, purpose: u8) {
        self.wiring.to_session(SessionInput::Mutate { author, owner, req_id, epoch: self.published().epoch, path, body, purpose });
    }

    /// One `Mutate` of a direct call: the outcome comes to the waiter, a repeat is the business of its caller.
    pub fn mutate_for_call(&self, owner: Option<crate::owners::session::MutateOwner>, path: String, body: String, purpose: u8) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        let author = WAITER_AUTHORS + waiter.id().raw();
        let (wiring, id) = (self.wiring.clone(), waiter.id());
        self.wiring.on_mutate_answer_once(author, move |outcome| { wiring.outcomes.settle(id, LifeReply::Mutated(outcome)); });
        self.mutate(author, owner, crate::owners::contract::next_request_id(), path, body, purpose);
        waiter
    }

    pub fn on_mutate_answer(&self, author: AuthorId, sink: impl FnMut(RequestId, AuthorOutcome) + Send + 'static) { self.wiring.on_mutate_answer(author, sink); }

    pub fn on_store_reply(&self, author: BatchAuthor, sink: impl Fn(BatchId, StoreReply) + Send + Sync + 'static) { self.wiring.on_store_reply(author, sink); }

    /// A call of the platform to the owner Outbox: the answer is `LifeReply::Outbox` or the end of the epoch.
    fn outbox_call(&self, epoch: Epoch, input: impl FnOnce(u64) -> OutboxInput) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        if self.wiring.addresses().is_none_or(|addresses| addresses.outbox.is_none()) { return self.settled(LifeReply::Outbox(OutboxAnswer::Err(Fault::NoSession))); }
        let waiter = self.wiring.outcomes.register(epoch);
        self.wiring.to_outbox_at(EpochStamp::Epoch(epoch), input(waiter.id().raw()));
        waiter
    }

    pub fn submit(&self, epoch: Epoch, op: crate::owners::write::outbox::NewOp) -> Waiter<LifeReply> { self.outbox_call(epoch, |reply| OutboxInput::Submit { epoch, op, reply }) }

    pub fn retry_operation(&self, epoch: Epoch, model: String, row_id: String) -> Waiter<LifeReply> { self.outbox_call(epoch, |reply| OutboxInput::Retry { model, row_id, reply }) }

    /// The id a row has now: a temporary id of a `create` that landed gives the real one.
    pub fn resolve_row(&self, epoch: Epoch, model: String, row: String) -> Waiter<LifeReply> { self.outbox_call(epoch, |reply| OutboxInput::Resolve { model, row, reply }) }

    /// The root row the answer of an operation landed.
    pub fn landed_row(&self, epoch: Epoch, op_id: u64) -> Waiter<LifeReply> { self.outbox_call(epoch, |reply| OutboxInput::Landed { op_id, reply }) }

    /// Answers when no operation of the owner Outbox is on its way.
    pub fn operations_quiet(&self, epoch: Epoch) -> Waiter<LifeReply> { self.outbox_call(epoch, |reply| OutboxInput::WhenQuiet { reply }) }

    pub fn discard_operation(&self, epoch: Epoch, model: String, row_id: String) -> Waiter<LifeReply> { self.outbox_call(epoch, |reply| OutboxInput::Discard { model, row_id, reply }) }

    pub fn operation_outcome(&self, epoch: Epoch, receipt: crate::owners::write::outbox::Receipt) -> Waiter<LifeReply> { self.outbox_call(epoch, |reply| OutboxInput::Outcome { receipt, reply }) }

    pub fn local_write(&self, epoch: Epoch, model: String, write: crate::owners::write::outbox::LocalWriteKind, row: String, values: String, now: crate::runtime::Time) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        let waiter = self.wiring.outcomes.register(epoch);
        self.wiring.to_local_writes_at(EpochStamp::Epoch(epoch), OutboxInput::LocalWrite { model, epoch, write, row, values, reply: waiter.id().raw(), now });
        waiter
    }

    pub fn register_operations(&self, declarations: Vec<(String, String)>) {
        self.wiring.to_transfer(crate::owners::write::transfer::TransferInput::RegisterOperations { declarations: declarations.clone() });
        self.wiring.to_outbox(OutboxInput::RegisterOperations { declarations });
    }

    pub fn declare(&self, names: Vec<(String, CallKind)>) { self.wiring.to_calls(CallsInput::Declare(names)); }

    pub fn session_reset(&self) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_lifecycle(LifecycleInput::SessionReset { reply: Some(waiter.id().raw()) });
        waiter
    }

    pub fn reset_models(&self, models: BTreeSet<String>) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_lifecycle(LifecycleInput::ResetModels { models, reply: Some(waiter.id().raw()) });
        waiter
    }

    #[cfg(test)]
    pub fn credential_committed(&self, identity: u64) { self.wiring.to_lifecycle(LifecycleInput::CredentialCommitted(crate::owners::contract::CredentialCommitted { identity })); }

    pub fn close(&self) -> Waiter<LifeReply> {
        if self.shut.load(Ordering::SeqCst) { return self.settled(LifeReply::Closed); }
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_lifecycle(LifecycleInput::Close { reply: waiter.id().raw() });
        waiter
    }
}
