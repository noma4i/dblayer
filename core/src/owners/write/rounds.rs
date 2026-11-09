//! Owner `Rounds`: the background round of the system job of Android.
//! One instance without an epoch: a round outlives the epoch of Transfer. It asks the platform for a job, tracks it by `request_id` and `round_id`,
//! and tells Transfer a refusal of the platform by `RoundStatus`.

use std::collections::{BTreeMap, BTreeSet};

use super::transfer::{TransferId, TransferInput, Transfers};
use crate::owners::contract::{Drained, EpochEnded, EpochStarted};
use crate::runtime::{Address, Cause, Effect, Effects, Epoch, Outbound, StateOwner, Time};

pub type RoundId = u32;
use crate::owners::contract::RequestId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoundsInput {
    Needed { epoch: Epoch, ids: Vec<TransferId> },
    Idle { epoch: Epoch, transfer_id: TransferId },
    Foreground,
    /// The platform gave the sink of the jobs (`set_round_jobs`).
    JobsSinkSet,
    RoundStarted { request_id: RequestId, round_id: RoundId, epoch: Epoch },
    RoundRefused { request_id: RequestId, epoch: Epoch, reason: String },
    RoundEnded { round_id: RoundId, epoch: Epoch },
    EpochStarted(EpochStarted),
    EpochEnded(EpochEnded),
    Closing,
    Clock(Time),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoundsEffect {
    ScheduleJob { request_id: RequestId, epoch: Epoch },
    EndJob { round_id: RoundId },
    Drained(Drained),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Round {
    None,
    Requested { epoch: Epoch, request_id: RequestId },
    Running { request_id: RequestId, round_id: RoundId, epoch: Epoch, next: Option<Epoch>, ended: bool },
    Refused { epoch: Epoch },
    Stopped { epoch: Epoch },
    Closed,
}

#[derive(Debug)]
pub struct RoundsState {
    transfer: Address<Transfers>,
    pub current_epoch: Epoch,
    pub active: BTreeMap<Epoch, BTreeSet<TransferId>>,
    pub round: Round,
    /// False until the platform sets the sink of the jobs: no job is asked for before.
    pub sink: bool,
    next_request: RequestId,
    drained: bool,
    #[cfg(test)]
    pub(crate) needed_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

type Fx = Effects<Rounds>;

impl RoundsState {
    pub fn new(transfer: Address<Transfers>) -> Self { Self { transfer, current_epoch: 0, active: BTreeMap::new(), round: Round::None, sink: false, next_request: 1, drained: false, #[cfg(test)] needed_count: Default::default() } }

    fn busy(&self, epoch: Epoch) -> bool { self.active.get(&epoch).is_some_and(|ids| !ids.is_empty()) }

    fn request(&mut self, epoch: Epoch, fx: &mut Fx) {
        let request_id = self.next_request;
        self.next_request += 1;
        self.round = Round::Requested { epoch, request_id };
        fx.push(Effect::Run(RoundsEffect::ScheduleJob { request_id, epoch }));
    }

    fn status(&self, epoch: Epoch, refused: Option<String>, fx: &mut Fx) {
        let transfer_ids = self.active.get(&epoch).map(|ids| ids.iter().copied().collect()).unwrap_or_default();
        fx.push(Effect::Send(Outbound::new(self.transfer, TransferInput::RoundStatus { epoch, transfer_ids, refused })));
    }

    /// A job is asked for the epoch again: the refusal on the transfers goes.
    fn request_again(&mut self, epoch: Epoch, fx: &mut Fx) {
        if matches!(self.round, Round::Refused { epoch: refused } if refused == epoch) {
            self.status(epoch, None, fx);
        }
        self.request(epoch, fx);
    }

    fn needed(&mut self, epoch: Epoch, ids: Vec<TransferId>, fx: &mut Fx) {
        #[cfg(test)] self.needed_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if epoch != self.current_epoch || self.round == Round::Closed {
            return;
        }
        self.active.entry(epoch).or_default().extend(ids);
        if !self.sink {
            return;
        }
        match self.round {
            Round::None => self.request(epoch, fx),
            Round::Requested { epoch: asked, .. } if asked != epoch => self.request(epoch, fx),
            Round::Refused { .. } | Round::Stopped { .. } => self.request_again(epoch, fx),
            Round::Running { request_id, round_id, epoch: running, ended, .. } if running != epoch => self.round = Round::Running { request_id, round_id, epoch: running, next: Some(epoch), ended },
            _ => {}
        }
    }

    fn foreground(&mut self, fx: &mut Fx) {
        if !self.sink {
            return;
        }
        let epoch = self.current_epoch;
        let waits = match self.round {
            Round::None => true,
            Round::Refused { epoch: old } | Round::Stopped { epoch: old } => old == epoch,
            _ => false,
        };
        if waits && self.busy(epoch) {
            self.request_again(epoch, fx);
        }
    }

    /// `JobsSinkSet`: the work that waited for the sink asks for its job now.
    fn sink_set(&mut self, fx: &mut Fx) {
        if std::mem::replace(&mut self.sink, true) {
            return;
        }
        let epoch = self.current_epoch;
        if self.round == Round::None && self.busy(epoch) {
            self.request(epoch, fx);
        }
    }

    fn started(&mut self, request_id: RequestId, round_id: RoundId, epoch: Epoch, fx: &mut Fx) {
        match self.round {
            Round::Requested { epoch: asked, request_id: current } if current == request_id && asked == epoch => {
                if self.busy(epoch) {
                    self.round = Round::Running { request_id, round_id, epoch, next: None, ended: false };
                } else {
                    self.round = Round::None;
                    fx.push(Effect::Run(RoundsEffect::EndJob { round_id }));
                }
            }
            Round::Running { request_id: running_request, round_id: running, epoch: running_epoch, .. } if (running_request, running, running_epoch) == (request_id, round_id, epoch) => {}
            _ => fx.push(Effect::Run(RoundsEffect::EndJob { round_id })),
        }
    }

    fn idle(&mut self, epoch: Epoch, transfer_id: TransferId, fx: &mut Fx) {
        let Some(ids) = self.active.get_mut(&epoch) else { return };
        if !ids.remove(&transfer_id) || !ids.is_empty() {
            return;
        }
        if let Round::Running { round_id, epoch: running, next, ended: false, .. } = self.round {
            if running == epoch {
                fx.push(Effect::Run(RoundsEffect::EndJob { round_id }));
                self.round = Round::None;
                if let Some(next) = next.filter(|next| self.busy(*next)) {
                    self.request(next, fx);
                }
            }
        }
    }

    fn refused(&mut self, request_id: RequestId, epoch: Epoch, reason: String, fx: &mut Fx) {
        if let Round::Requested { epoch: asked, request_id: current } = self.round {
            if current == request_id && asked == epoch {
                self.round = Round::Refused { epoch };
                self.status(epoch, Some(reason), fx);
            }
        }
    }

    fn ended(&mut self, round_id: RoundId, epoch: Epoch, fx: &mut Fx) {
        let Round::Running { round_id: running, epoch: running_epoch, next, ended, .. } = self.round else { return };
        if running != round_id || running_epoch != epoch {
            return;
        }
        match next {
            Some(next) if self.busy(next) => self.request(next, fx),
            _ if self.busy(epoch) && !ended => self.round = Round::Stopped { epoch },
            _ => self.round = Round::None,
        }
    }

    fn epoch_ended(&mut self, epoch: Epoch) {
        self.active.remove(&epoch);
        match self.round {
            Round::Requested { epoch: found, .. } | Round::Refused { epoch: found } | Round::Stopped { epoch: found } if found == epoch => self.round = Round::None,
            Round::Running { request_id, round_id, epoch: found, next, .. } if found == epoch => self.round = Round::Running { request_id, round_id, epoch, next, ended: true },
            Round::Running { request_id, round_id, epoch: found, next: Some(next), ended } if next == epoch => self.round = Round::Running { request_id, round_id, epoch: found, next: None, ended },
            _ => {}
        }
    }

    fn closing(&mut self, fx: &mut Fx) {
        if let Round::Running { round_id, .. } = self.round {
            fx.push(Effect::Run(RoundsEffect::EndJob { round_id }));
        }
        self.round = Round::Closed;
        if !std::mem::replace(&mut self.drained, true) {
            fx.push(Effect::Run(RoundsEffect::Drained(Drained { owner: "rounds" })));
        }
    }
}

pub struct Rounds;

impl StateOwner for Rounds {
    const EPOCHED: bool = false;

    type State = RoundsState;
    type Input = RoundsInput;
    type Effect = RoundsEffect;

    fn step(state: &mut RoundsState, input: RoundsInput) -> Effects<Self> {
        let mut fx: Fx = Vec::new();
        if state.round == Round::Closed {
            if let RoundsInput::RoundStarted { round_id, .. } = input {
                fx.push(Effect::Run(RoundsEffect::EndJob { round_id }));
            }
            return fx;
        }
        match input {
            RoundsInput::Needed { epoch, ids } => state.needed(epoch, ids, &mut fx),
            RoundsInput::Idle { epoch, transfer_id } => state.idle(epoch, transfer_id, &mut fx),
            RoundsInput::Foreground => state.foreground(&mut fx),
            RoundsInput::JobsSinkSet => state.sink_set(&mut fx),
            RoundsInput::RoundStarted { request_id, round_id, epoch } => state.started(request_id, round_id, epoch, &mut fx),
            RoundsInput::RoundRefused { request_id, epoch, reason } => state.refused(request_id, epoch, reason, &mut fx),
            RoundsInput::RoundEnded { round_id, epoch } => state.ended(round_id, epoch, &mut fx),
            RoundsInput::EpochStarted(started) => state.current_epoch = started.epoch,
            RoundsInput::EpochEnded(ended) => state.epoch_ended(ended.epoch),
            RoundsInput::Closing => state.closing(&mut fx),
            RoundsInput::Clock(_) => {}
        }
        fx
    }

    fn tick(now: Time) -> RoundsInput { RoundsInput::Clock(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> RoundsInput { RoundsInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> RoundsInput { RoundsInput::Closing }
}
