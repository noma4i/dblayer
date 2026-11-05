use std::sync::mpsc;

use super::*;

pub struct Toy;

#[derive(Clone, Debug, PartialEq)]
pub enum ToyInput {
    Go,
    Deposit(u32),
    Seal,
    WorkRequest(u32),
    Result(u32),
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
    Settle(OutcomeId, u32),
    EndOutcomes(Epoch, Cause),
    CloseOutcomes,
    Report(u32),
}

#[derive(Clone, Default)]
pub struct ToyState {
    pub on_go: Option<(Address<Toy>, ToyInput)>,
    pub total: u32,
    pub lost: u32,
    pub sealed: bool,
    pub results: Vec<u32>,
    pub fired: Vec<u32>,
    pub ticks: Vec<Time>,
    pub ended: Vec<(Epoch, Cause)>,
    pub closed: bool,
}

impl Owner for Toy {
    type State = ToyState;
    type Input = ToyInput;
    type Effect = ToyEffect;

    fn step(state: &mut ToyState, input: ToyInput) -> Effects<Toy> {
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
            ToyInput::Seal => {
                state.sealed = true;
                vec![]
            }
            ToyInput::WorkRequest(n) => vec![Effect::Run(ToyEffect::Work(n))],
            ToyInput::Result(n) => {
                state.results.push(n);
                vec![]
            }
            ToyInput::Resolve(id, value) => vec![Effect::Run(ToyEffect::Settle(id, value))],
            ToyInput::ScheduleAt(id, at, n) => vec![Effect::Schedule { id: ScheduleId(id), at, input: ToyInput::Fire(n) }],
            ToyInput::CancelSchedule(id) => vec![Effect::Cancel(ScheduleId(id))],
            ToyInput::Fire(n) => {
                state.fired.push(n);
                vec![Effect::Run(ToyEffect::Report(n))]
            }
            ToyInput::Tick(now) => {
                state.ticks.push(now);
                vec![]
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
}

impl EffectRunner<Toy> for ToyRunner {
    fn run(&mut self, _stamp: Stamp, effect: ToyEffect, replies: &mut Vec<ToyInput>) {
        match effect {
            ToyEffect::Work(n) => replies.push(ToyInput::Result(n * 2)),
            ToyEffect::Settle(id, value) => {
                if let Some(outcomes) = &self.outcomes {
                    outcomes.settle(id, value);
                }
            }
            ToyEffect::EndOutcomes(epoch, cause) => {
                if let Some(outcomes) = &self.outcomes {
                    outcomes.end_epoch(epoch, cause);
                }
            }
            ToyEffect::CloseOutcomes => {
                if let Some(outcomes) = &self.outcomes {
                    outcomes.close();
                }
            }
            ToyEffect::Report(n) => {
                if let Some(report) = &self.report {
                    let _ = report.send(n);
                }
            }
        }
    }
}

pub fn race(seed: u64) -> (DetScheduler, Address<Toy>) {
    let mut det = DetScheduler::new(seed);
    let ledger = det.add::<Toy>("ledger", ToyState::default(), ToyRunner::default());
    let depositor = det.add::<Toy>("depositor", ToyState { on_go: Some((ledger, ToyInput::Deposit(5))), ..ToyState::default() }, ToyRunner::default());
    let sealer = det.add::<Toy>("sealer", ToyState { on_go: Some((ledger, ToyInput::Seal)), ..ToyState::default() }, ToyRunner::default());
    det.start_epoch(1);
    det.send(depositor, Stamp::Epoch(1), ToyInput::Go);
    det.send(sealer, Stamp::Epoch(1), ToyInput::Go);
    (det, ledger)
}
