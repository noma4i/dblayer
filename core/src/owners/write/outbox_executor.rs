//! Executor of the effects of `Outbox`: it settles the answer of a call.
//! Store batches, Session `Mutate`, `MutationOutcome` and timers are `Effect::Send` and `Effect::Schedule` of the owner.

use std::sync::Arc;

use super::answer::{DecodeJob, Decoded};
use super::outbox::{Outbox, OutboxAnswer, OutboxEffect, OutboxInput};
use crate::owners::contract::{Drained, ReplyId};
use crate::runtime::{Back, EffectRunner, Epoch, EpochStamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboxSignal {
    Started(Epoch),
    SnapshotRequest(Epoch),
    RestoreClassified(Epoch),
}

pub type MarkStale = Arc<dyn Fn(Vec<(String, String)>, i64) + Send + Sync>;
/// Reads the record of a receipt outside the actor and settles the reply with the answer of the owner (`owner`, `id`, `cycle_id`, `reply`).
pub type ReadOutcome = Arc<dyn Fn(crate::owners::store::OutcomeOwner, u64, u64, ReplyId) + Send + Sync>;
pub type DecodeAnswer = Arc<dyn Fn(&DecodeJob) -> Decoded + Send + Sync>;

pub struct OutboxExecutor {
    pub settle: Arc<dyn Fn(ReplyId, OutboxAnswer) + Send + Sync>,
    /// `Drained` goes to Lifecycle.
    pub drained: Arc<dyn Fn(Drained) + Send + Sync>,
    pub signal: Arc<dyn Fn(OutboxSignal) + Send + Sync>,
    /// Marks the windows of the (model, relation) pairs stale in memory.
    pub stale: Option<MarkStale>,
    /// Reads the answer of an operation; `None` refuses every answer. It runs on a thread of the executor, not on the actor.
    pub decode: Option<(tokio::runtime::Handle, DecodeAnswer)>,
    /// The barrier of the owners waits for the jobs of this executor.
    pub counted: Option<crate::runtime::Handle>,
    pub read_outcome: Option<ReadOutcome>,
}

impl EffectRunner<Outbox> for OutboxExecutor {
    fn run(&mut self, stamp: EpochStamp, effect: OutboxEffect, back: &Back<Outbox>) {
        match effect {
            OutboxEffect::Reply { reply, answer } => (self.settle)(reply, answer),
            OutboxEffect::Drained(drained) => (self.drained)(drained),
            OutboxEffect::Started { epoch } => (self.signal)(OutboxSignal::Started(epoch)),
            OutboxEffect::SnapshotRequest { epoch } => (self.signal)(OutboxSignal::SnapshotRequest(epoch)),
            OutboxEffect::RestoreClassified { epoch } => (self.signal)(OutboxSignal::RestoreClassified(epoch)),
            OutboxEffect::ReadOutcome { reply, op_id, cycle_id } => match &self.read_outcome {
                Some(read) => read(crate::owners::store::OutcomeOwner::Operation, op_id, cycle_id, reply),
                None => (self.settle)(reply, OutboxAnswer::Err(super::outbox::Fault::NotFound)),
            },
            OutboxEffect::Stale { families, revision } => {
                if let Some(stale) = &self.stale { stale(families, revision); }
            }
            OutboxEffect::Decode(job) => {
                let Some((handle, decode)) = self.decode.clone() else {
                    return back.reply(stamp, OutboxInput::Refused { op_id: job.op_id, temporary: false, detail: "the answer cannot be read".into() });
                };
                let back = back.clone();
                let launch = self.counted.as_ref().map(crate::runtime::Handle::launch);
                drop(crate::task::spawn_blocking_on(&handle, launch, move || {
                    let input = match decode(&job) {
                        Decoded::MissingDestroy { stale } => OutboxInput::MissingDestroy { op_id: job.op_id, stale },
                        Decoded::Land { rows, swaps, stale, result, missing } => OutboxInput::Response { op_id: job.op_id, rows, swaps, stale, result, missing, errors: None },
                        Decoded::Unreadable { stale } => OutboxInput::Unreadable { op_id: job.op_id, stale },
                        Decoded::Refused { temporary, detail } => OutboxInput::Refused { op_id: job.op_id, temporary, detail },
                    };
                    back.reply(stamp, input);
                }));
            }
        }
    }
}
