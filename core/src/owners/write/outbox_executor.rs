//! Executor of the effects of `Outbox` (`design.md` F3). In this step it does the IO the platform sees: it settles the answer of a call.
//! Effects for other owners (Store batches, Session `Mutate`, `MutationOutcome`, `Drained`) and timers are `Effect::Send` and `Effect::Schedule`
//! of the owner in the next step; until then the executor leaves them alone.

use std::sync::Arc;

use super::outbox::{Outbox, OutboxAnswer, OutboxEffect};
use crate::owners::contract::ReplyId;
use crate::runtime::{Back, EffectRunner, EpochStamp};

pub struct OutboxExecutor {
    pub settle: Arc<dyn Fn(ReplyId, OutboxAnswer) + Send + Sync>,
}

impl EffectRunner<Outbox> for OutboxExecutor {
    fn run(&mut self, _stamp: EpochStamp, effect: OutboxEffect, _back: &Back<Outbox>) {
        match effect {
            OutboxEffect::Reply { reply, answer } => (self.settle)(reply, answer),
            OutboxEffect::Store { .. } | OutboxEffect::Send { .. } | OutboxEffect::CancelRequest { .. } | OutboxEffect::TransferOutcome { .. } | OutboxEffect::ScheduleRetry { .. } | OutboxEffect::Drained(_) => {}
        }
    }
}
