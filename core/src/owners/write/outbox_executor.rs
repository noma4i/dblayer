//! Executor of the effects of `Outbox`: it settles the answer of a call.
//! Store batches, Session `Mutate`, `MutationOutcome` and timers are `Effect::Send` and `Effect::Schedule` of the owner.

use std::sync::Arc;

use super::answer::{DecodeJob, Decoded};
use super::outbox::{Outbox, OutboxAnswer, OutboxEffect, OutboxInput};
use crate::owners::contract::{Drained, ReplyId};
use crate::runtime::{Back, EffectRunner, EpochStamp};

pub struct OutboxExecutor {
    pub settle: Arc<dyn Fn(ReplyId, OutboxAnswer) + Send + Sync>,
    /// `Drained` goes to Lifecycle.
    pub drained: Arc<dyn Fn(Drained) + Send + Sync>,
    /// Copies a file into the directory of the files; `None` fails every copy.
    pub copy: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,
    /// Marks the windows of the (model, relation) pairs stale in memory.
    pub stale: Option<Arc<dyn Fn(Vec<(String, String)>, i64) + Send + Sync>>,
    /// Reads the answer of an operation; `None` refuses every answer. It runs on a thread of the executor, not on the actor.
    pub decode: Option<(tokio::runtime::Handle, Arc<dyn Fn(&DecodeJob) -> Decoded + Send + Sync>)>,
    /// The barrier of the owners waits for the jobs of this executor.
    pub counted: Option<crate::runtime::Handle>,
}

impl EffectRunner<Outbox> for OutboxExecutor {
    fn run(&mut self, stamp: EpochStamp, effect: OutboxEffect, back: &Back<Outbox>) {
        match effect {
            OutboxEffect::Reply { reply, answer } => (self.settle)(reply, answer),
            OutboxEffect::Drained(drained) => (self.drained)(drained),
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
                drop(crate::task::spawn_blocking_on(crate::task::Carry::Detached, &handle, move || {
                    let _launch = launch;
                    let input = match decode(&job) {
                        Decoded::Land { rows, swaps, stale, result } => OutboxInput::Response { op_id: job.op_id, rows, swaps, stale, result, errors: None },
                        Decoded::Refused { temporary, detail } => OutboxInput::Refused { op_id: job.op_id, temporary, detail },
                    };
                    back.reply(stamp, input);
                }));
            }
        }
    }
}
