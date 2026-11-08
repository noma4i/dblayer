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

pub type CopyFile = Arc<dyn Fn(&str) -> bool + Send + Sync>;
pub type MarkStale = Arc<dyn Fn(Vec<(String, String)>, i64) + Send + Sync>;
pub type DecodeAnswer = Arc<dyn Fn(&DecodeJob) -> Decoded + Send + Sync>;

pub struct OutboxExecutor {
    pub settle: Arc<dyn Fn(ReplyId, OutboxAnswer) + Send + Sync>,
    /// `Drained` goes to Lifecycle.
    pub drained: Arc<dyn Fn(Drained) + Send + Sync>,
    pub signal: Arc<dyn Fn(OutboxSignal) + Send + Sync>,
    /// Copies a file into the directory of the files; `None` fails every copy.
    pub copy: Option<CopyFile>,
    /// Marks the windows of the (model, relation) pairs stale in memory.
    pub stale: Option<MarkStale>,
    /// Reads the answer of an operation; `None` refuses every answer. It runs on a thread of the executor, not on the actor.
    pub decode: Option<(tokio::runtime::Handle, DecodeAnswer)>,
    /// The barrier of the owners waits for the jobs of this executor.
    pub counted: Option<crate::runtime::Handle>,
}

impl EffectRunner<Outbox> for OutboxExecutor {
    fn run(&mut self, stamp: EpochStamp, effect: OutboxEffect, back: &Back<Outbox>) {
        match effect {
            OutboxEffect::Reply { reply, answer } => (self.settle)(reply, answer),
            OutboxEffect::Drained(drained) => (self.drained)(drained),
            OutboxEffect::Started { epoch } => (self.signal)(OutboxSignal::Started(epoch)),
            OutboxEffect::SnapshotRequest { epoch } => (self.signal)(OutboxSignal::SnapshotRequest(epoch)),
            OutboxEffect::RestoreClassified { epoch } => (self.signal)(OutboxSignal::RestoreClassified(epoch)),
            OutboxEffect::Stale { families, revision } => {
                if let Some(stale) = &self.stale { stale(families, revision); }
            }
            OutboxEffect::Copy { id, source } => {
                let copied = self.copy.as_ref().is_some_and(|copy| copy(&source));
                back.reply(stamp, if copied { OutboxInput::CopyDone { id } } else { OutboxInput::CopyFailed { id } });
            }
            OutboxEffect::Decode(job) => {
                let Some((handle, decode)) = self.decode.clone() else {
                    return back.reply(stamp, OutboxInput::Refused { op_id: job.op_id, temporary: false, detail: "the answer cannot be read".into() });
                };
                let back = back.clone();
                let launch = self.counted.as_ref().map(crate::runtime::Handle::launch);
                drop(crate::task::spawn_blocking_on(&handle, launch, move || {
                    let input = match decode(&job) {
                        Decoded::Land { rows, swaps, stale, result, missing } => OutboxInput::Response { op_id: job.op_id, rows, swaps, stale, result, missing, errors: None },
                        Decoded::Refused { temporary, detail } => OutboxInput::Refused { op_id: job.op_id, temporary, detail },
                    };
                    back.reply(stamp, input);
                }));
            }
        }
    }
}
