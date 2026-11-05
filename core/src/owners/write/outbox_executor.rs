//! Executor of the effects of `Outbox` (`design.md` F3): it settles the answer of a call.
//! Store batches, Session `Mutate`, `MutationOutcome` and timers are `Effect::Send` and `Effect::Schedule` of the owner.

use std::sync::Arc;

use super::outbox::{Outbox, OutboxAnswer, OutboxEffect, OutboxInput};
use crate::owners::contract::{Drained, ReplyId};
use crate::runtime::{Back, EffectRunner, EpochStamp};

pub struct OutboxExecutor {
    pub settle: Arc<dyn Fn(ReplyId, OutboxAnswer) + Send + Sync>,
    /// `Drained` goes to Lifecycle.
    pub drained: Arc<dyn Fn(Drained) + Send + Sync>,
    /// Copies a file into the directory of the files; `None` fails every copy.
    pub copy: Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>,
}

impl EffectRunner<Outbox> for OutboxExecutor {
    fn run(&mut self, stamp: EpochStamp, effect: OutboxEffect, back: &Back<Outbox>) {
        match effect {
            OutboxEffect::Reply { reply, answer } => (self.settle)(reply, answer),
            OutboxEffect::Drained(drained) => (self.drained)(drained),
            OutboxEffect::Copy { id, source } => {
                let copied = self.copy.as_ref().is_some_and(|copy| copy(&source));
                back.reply(stamp, if copied { OutboxInput::CopyDone { id } } else { OutboxInput::CopyFailed { id } });
            }
        }
    }
}
