use std::sync::{mpsc, Arc, Mutex, PoisonError};

use super::*;

pub const CLOSED: u32 = u32::MAX;

pub type ParkedBacks = Arc<Mutex<Vec<(Back<Toy>, EpochStamp, ToyInput)>>>;

pub struct Toy;

#[derive(Clone, Debug, PartialEq)]
pub enum ToyInput {
    Go,
    Tell,
    Deposit(u32),
    Seal,
    WorkRequest(u32),
    Result(u32),
    Note(u32),
    WorkAsync(u32),
    Release,
    Resolve(OutcomeId, u32),
    ScheduleAt(u64, Time, u32),
    CancelSchedule(u64),
    Fire(u32),
    Tick(Time),
    EpochEnded(Epoch, Cause),
    Closing,
}

#[derive(Debug)]
pub enum ToyEffect {
    Work(u32),
    Park(u32),
    ReleaseParked,
    Settle(OutcomeId, u32),
    EndOutcomes(Epoch, Cause),
    CloseOutcomes,
    Report(u32),
}

#[derive(Clone, Default)]
pub struct ToyState {
    pub on_go: Option<(Address<Toy>, ToyInput)>,
    pub tell: Option<(Address<Sink>, u32)>,
    pub on_tick: Option<(Address<Toy>, ToyInput)>,
    pub pump: Option<Address<Toy>>,
    pub total: u32,
    pub lost: u32,
    pub sealed: bool,
    pub results: Vec<u32>,
    pub fired: Vec<u32>,
    pub ticks: Vec<Time>,
    pub ended: Vec<(Epoch, Cause)>,
    pub closed: bool,
}

impl StateOwner for Toy {
    const EPOCHED: bool = true;

    type State = ToyState;
    type Input = ToyInput;
    type Effect = ToyEffect;

    fn step(state: &mut ToyState, input: ToyInput) -> Effects<Toy> {
        match input {
            ToyInput::Go => state.on_go.clone().map(|(to, message)| vec![Effect::Send(Outbound::new(to, message))]).unwrap_or_default(),
            ToyInput::Tell => state.tell.map(|(to, n)| vec![Effect::Send(Outbound::new(to, SinkInput::Note(n)))]).unwrap_or_default(),
            ToyInput::Deposit(amount) => {
                if state.sealed {
                    state.lost += amount;
                } else {
                    state.total += amount;
                }
                vec![]
            }
            ToyInput::Seal => {
                state.sealed = true;
                vec![]
            }
            ToyInput::WorkRequest(n) => vec![Effect::Run(ToyEffect::Work(n))],
            ToyInput::Result(n) => {
                state.results.push(n);
                vec![Effect::Run(ToyEffect::Report(n))]
            }
            ToyInput::Note(n) => {
                state.results.push(n);
                vec![]
            }
            ToyInput::WorkAsync(n) => {
                let mut effects = vec![Effect::Run(ToyEffect::Park(n))];
                effects.extend(state.pump.map(|pump| Effect::Send(Outbound::new(pump, ToyInput::Release))));
                effects
            }
            ToyInput::Release => vec![Effect::Run(ToyEffect::ReleaseParked)],
            ToyInput::Resolve(id, value) => vec![Effect::Run(ToyEffect::Settle(id, value))],
            ToyInput::ScheduleAt(id, at, n) => vec![Effect::Schedule { id: ScheduleId(id), after: std::time::Duration::from_millis(at.0), input: ToyInput::Fire(n) }],
            ToyInput::CancelSchedule(id) => vec![Effect::Cancel(ScheduleId(id))],
            ToyInput::Fire(n) => {
                state.fired.push(n);
                vec![Effect::Run(ToyEffect::Report(n))]
            }
            ToyInput::Tick(now) => {
                state.ticks.push(now);
                state.on_tick.clone().map(|(to, message)| vec![Effect::Send(Outbound::new(to, message))]).unwrap_or_default()
            }
            ToyInput::EpochEnded(epoch, cause) => {
                state.ended.push((epoch, cause));
                vec![Effect::Run(ToyEffect::EndOutcomes(epoch, cause))]
            }
            ToyInput::Closing => {
                state.closed = true;
                vec![Effect::Run(ToyEffect::CloseOutcomes)]
            }
        }
    }

    fn tick(now: Time) -> ToyInput { ToyInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> ToyInput { ToyInput::EpochEnded(epoch, cause) }

    fn closing() -> ToyInput { ToyInput::Closing }
}

#[derive(Default)]
pub struct ToyRunner {
    pub outcomes: Option<Outcomes<u32>>,
    pub report: Option<mpsc::Sender<u32>>,
    pub pending: Option<ParkedBacks>,
}

impl EffectRunner<Toy> for ToyRunner {
    fn run(&mut self, stamp: EpochStamp, effect: ToyEffect, back: &Back<Toy>) {
        match effect {
            ToyEffect::Work(n) => back.reply(stamp, ToyInput::Result(n * 2)),
            ToyEffect::Park(n) => {
                if let Some(pending) = &self.pending {
                    pending.lock().unwrap_or_else(PoisonError::into_inner).push((back.clone(), stamp, ToyInput::Result(n * 2)));
                }
            }
            ToyEffect::ReleaseParked => {
                if let Some(pending) = &self.pending {
                    let parked = std::mem::take(&mut *pending.lock().unwrap_or_else(PoisonError::into_inner));
                    for (return_to, parked_stamp, input) in parked {
                        return_to.reply(parked_stamp, input);
                    }
                }
            }
            ToyEffect::Settle(id, value) => {
                if let Some(outcomes) = &self.outcomes {
                    outcomes.settle(id, value);
                }
            }
            ToyEffect::EndOutcomes(epoch, cause) => {
                if let Some(outcomes) = &self.outcomes {
                    outcomes.end_epoch(epoch, cause);
                }
                if let Some(report) = &self.report {
                    let _ = crate::db::lock::effect(|| report.send(epoch as u32));
                }
            }
            ToyEffect::CloseOutcomes => {
                if let Some(outcomes) = &self.outcomes {
                    outcomes.close();
                }
                if let Some(report) = &self.report {
                    let _ = crate::db::lock::effect(|| report.send(CLOSED));
                }
            }
            ToyEffect::Report(n) => {
                if let Some(report) = &self.report {
                    let _ = crate::db::lock::effect(|| report.send(n));
                }
            }
        }
    }
}

pub struct Hub;

#[derive(Debug)]
pub enum HubInput {
    Begin(Epoch),
    Finish(Epoch, Cause),
    Shutdown,
    Probe(Address<Toy>),
    Idle,
}

#[derive(Debug)]
pub enum HubEffect {}

impl StateOwner for Hub {
    const EPOCHED: bool = false;

    type State = ();
    type Input = HubInput;
    type Effect = HubEffect;

    fn step(_state: &mut (), input: HubInput) -> Effects<Hub> {
        match input {
            HubInput::Begin(epoch) => vec![Effect::Broadcast(Control::EpochStarted(epoch))],
            HubInput::Finish(epoch, cause) => vec![Effect::Broadcast(Control::EpochEnded(epoch, cause))],
            HubInput::Shutdown => vec![Effect::Broadcast(Control::Closing)],
            HubInput::Probe(to) => vec![Effect::Send(Outbound::new(to, ToyInput::Deposit(1)))],
            HubInput::Idle => vec![],
        }
    }

    fn tick(_now: Time) -> HubInput { HubInput::Idle }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> HubInput { HubInput::Idle }

    fn closing() -> HubInput { HubInput::Idle }
}

pub struct HubRunner;

impl EffectRunner<Hub> for HubRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: HubEffect, _back: &Back<Hub>) { match effect {} }
}

pub fn race(seed: u64) -> (DetScheduler, Address<Toy>) {
    let mut det = DetScheduler::new(seed);
    let ledger = det.add::<Toy>("ledger", ToyState::default(), ToyRunner::default());
    let depositor = det.add::<Toy>("depositor", ToyState { on_go: Some((ledger, ToyInput::Deposit(5))), ..ToyState::default() }, ToyRunner::default());
    let sealer = det.add::<Toy>("sealer", ToyState { on_go: Some((ledger, ToyInput::Seal)), ..ToyState::default() }, ToyRunner::default());
    det.start_epoch(1);
    det.post(depositor, EpochStamp::Epoch(1), ToyInput::Go);
    det.post(sealer, EpochStamp::Epoch(1), ToyInput::Go);
    (det, ledger)
}

pub struct Sink;

#[derive(Debug)]
pub enum SinkInput {
    Note(u32),
    Idle,
}

#[derive(Debug)]
pub enum SinkEffect {}

impl StateOwner for Sink {
    const EPOCHED: bool = false;

    type State = Vec<u32>;
    type Input = SinkInput;
    type Effect = SinkEffect;

    fn step(state: &mut Vec<u32>, input: SinkInput) -> Effects<Sink> {
        if let SinkInput::Note(n) = input {
            state.push(n);
        }
        vec![]
    }

    fn tick(_now: Time) -> SinkInput { SinkInput::Idle }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> SinkInput { SinkInput::Idle }

    fn closing() -> SinkInput { SinkInput::Idle }
}

pub struct SinkRunner;

impl EffectRunner<Sink> for SinkRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: SinkEffect, _back: &Back<Sink>) { match effect {} }
}
