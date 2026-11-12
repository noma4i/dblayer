use std::sync::{mpsc, Arc, Mutex, PoisonError};

use super::*;

pub const CLOSED: u32 = u32::MAX;

pub type ParkedBacks = Arc<Mutex<Vec<(Back<Toy>, EpochStamp, ToyInput)>>>;

pub struct Toy;

#[derive(Clone, Debug, PartialEq)]
pub enum ToyInput {
    Go,
    Deposit(u32),
    WorkRequest(u32),
    Result(u32),
    Note(u32),
    WorkAsync(u32),
    Release,
    ScheduleAt(u64, Time, u32),
    Fire(u32),
    Burst(Vec<u32>),
    ExecutorFailed(Vec<u64>),
    EpochStarted(Epoch),
    EpochEnded(Epoch, Cause),
    Closing,
}

#[derive(Debug)]
pub enum ToyEffect {
    Work(u32),
    Park(u32),
    ReleaseParked,
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
    pub failed: Vec<u64>,
    pub fired: Vec<u32>,
    pub ticks: Vec<Time>,
    pub nows: Vec<Time>,
    pub ended: Vec<(Epoch, Cause)>,
    pub started: Vec<Epoch>,
    pub closed: bool,
}

impl StateOwner for Toy {
    const EPOCHED: bool = true;

    type State = ToyState;
    type Input = ToyInput;
    type Effect = ToyEffect;

    fn step(state: &mut ToyState, input: ToyInput, now: Time) -> Effects<Toy> {
        state.nows.push(now);
        match input {
            ToyInput::Go => state.on_go.clone().map(|(to, message)| vec![Effect::Send(Outbound::new(to, message))]).unwrap_or_default(),
            ToyInput::Deposit(amount) => {
                if state.sealed {
                    state.lost += amount;
                } else {
                    state.total += amount;
                }
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
            ToyInput::ScheduleAt(id, at, n) => vec![Effect::Schedule { id: ScheduleId(id), after: std::time::Duration::from_millis(at.0), input: ToyInput::Fire(n) }],
            ToyInput::Fire(n) => {
                state.fired.push(n);
                state.ticks.push(now);
                let mut effects = vec![Effect::Run(ToyEffect::Report(n))];
                effects.extend(state.on_tick.clone().map(|(to, message)| Effect::Send(Outbound::new(to, message))));
                effects
            }
            ToyInput::Burst(list) => list
                .into_iter()
                .flat_map(|n| {
                    let run = Effect::Run(ToyEffect::Report(n));
                    match state.tell {
                        Some((to, _)) => vec![run, Effect::Send(Outbound::new(to, SinkInput::Note(n)))],
                        None => vec![run],
                    }
                })
                .collect(),
            ToyInput::ExecutorFailed(ids) => {
                state.failed.extend(ids);
                vec![]
            }
            ToyInput::EpochStarted(epoch) => { state.started.push(epoch); vec![] }
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

    fn epoch_started(epoch: Epoch) -> Option<ToyInput> { Some(ToyInput::EpochStarted(epoch)) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> ToyInput { ToyInput::EpochEnded(epoch, cause) }

    fn closing() -> ToyInput { ToyInput::Closing }

    fn executor_failed(_executor: &str, pending: Vec<EffectId>) -> Vec<ToyInput> { vec![ToyInput::ExecutorFailed(pending)] }
}

#[derive(Default)]
pub struct ToyRunner {
    pub outcomes: Option<Outcomes<u32>>,
    pub report: Option<mpsc::Sender<u32>>,
    pub pending: Option<ParkedBacks>,
    pub calls: Option<Arc<Mutex<Vec<u32>>>>,
    pub held: Option<Arc<Mutex<Vec<&'static str>>>>,
}

impl EffectRunner<Toy> for ToyRunner {
    fn run(&mut self, stamp: EpochStamp, effect: ToyEffect, back: &Back<Toy>) {
        if let Some(held) = &self.held {
            held.lock().unwrap_or_else(PoisonError::into_inner).extend(crate::lock::held_classes());
        }
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
            ToyEffect::EndOutcomes(epoch, cause) => {
                if let Some(outcomes) = &self.outcomes {
                    outcomes.end_epoch(epoch, cause);
                }
                if let Some(report) = &self.report {
                    let _ = crate::lock::effect(|| report.send(epoch as u32));
                }
            }
            ToyEffect::CloseOutcomes => {
                if let Some(outcomes) = &self.outcomes {
                    outcomes.close();
                }
                if let Some(report) = &self.report {
                    let _ = crate::lock::effect(|| report.send(CLOSED));
                }
            }
            ToyEffect::Report(n) => {
                if let Some(calls) = &self.calls {
                    calls.lock().unwrap_or_else(PoisonError::into_inner).push(n);
                }
                if let Some(report) = &self.report {
                    let _ = crate::lock::effect(|| report.send(n));
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
    Idle,
}

#[derive(Debug)]
pub enum HubEffect {}

impl StateOwner for Hub {
    const EPOCHED: bool = false;

    type State = ();
    type Input = HubInput;
    type Effect = HubEffect;

    fn step(_state: &mut (), input: HubInput, _now: Time) -> Effects<Hub> {
        match input {
            HubInput::Begin(epoch) => vec![Effect::Broadcast(Control::EpochStarted(epoch))],
            HubInput::Finish(epoch, cause) => vec![Effect::Broadcast(Control::EpochEnded(epoch, cause))],
            HubInput::Shutdown => vec![Effect::Broadcast(Control::Closing)],
            HubInput::Idle => vec![],
        }
    }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> HubInput { HubInput::Idle }

    fn closing() -> HubInput { HubInput::Idle }
}

pub struct HubRunner;

impl EffectRunner<Hub> for HubRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: HubEffect, _back: &Back<Hub>) { match effect {} }
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

    fn step(state: &mut Vec<u32>, input: SinkInput, _now: Time) -> Effects<Sink> {
        if let SinkInput::Note(n) = input {
            state.push(n);
        }
        vec![]
    }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> SinkInput { SinkInput::Idle }

    fn closing() -> SinkInput { SinkInput::Idle }
}
