use statig::prelude::*;

use super::open::{locked, Db};
use super::upload_accepted::AcceptedUpload;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoundStep {
    More,
    Stale,
    Done,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Idle,
    Running,
    Ending,
}

#[derive(Debug, Clone)]
enum RoundEvent {
    Restore,
    Accept(Vec<AcceptedUpload>),
    Begin,
    Next(u32),
    Leave(u32),
    End(u32),
    Abandon(u32),
}

#[derive(Debug, Default)]
enum Reply {
    #[default]
    None,
    Accepted(bool),
    Began(Option<u32>),
    Step(RoundStep),
    Ended(Option<Vec<AcceptedUpload>>),
}

#[derive(Default)]
struct Cover {
    accepted: u64,
    seen: u64,
    round: u32,
    phase: Phase,
    late: Vec<AcceptedUpload>,
    reply: Reply,
}

impl Cover {
    fn owns(&self, owner: u32) -> bool {
        owner == self.round
    }

    fn start(&mut self) -> Outcome<State> {
        self.seen = self.accepted;
        self.round += 1;
        self.reply = Reply::Began(Some(self.round));
        Transition(State::running())
    }

    fn step(&mut self) -> Outcome<State> {
        if self.accepted == self.seen {
            self.reply = Reply::Step(RoundStep::Done);
            Transition(State::ending())
        } else {
            self.seen = self.accepted;
            self.reply = Reply::Step(RoundStep::More);
            Handled
        }
    }

    fn leave(&mut self) -> Outcome<State> {
        self.reply = Reply::Step(RoundStep::Done);
        Transition(State::ending())
    }

    fn abandon(&mut self) -> Outcome<State> {
        self.late.clear();
        self.round += 1;
        Transition(State::idle())
    }
}

struct RoundFlow;

#[state_machine(initial = "State::idle()")]
impl RoundFlow {
    #[superstate]
    fn any(context: &mut Cover, event: &RoundEvent) -> Outcome<State> {
        match event {
            RoundEvent::Restore => Transition(match context.phase {
                Phase::Idle => State::idle(),
                Phase::Running => State::running(),
                Phase::Ending => State::ending(),
            }),
            RoundEvent::Next(_) | RoundEvent::Leave(_) => {
                context.reply = Reply::Step(RoundStep::Stale);
                Handled
            }
            RoundEvent::End(_) => {
                context.reply = Reply::Ended(None);
                Handled
            }
            _ => Handled,
        }
    }

    #[state(superstate = "any")]
    fn idle(context: &mut Cover, event: &RoundEvent) -> Outcome<State> {
        match event {
            RoundEvent::Accept(_) => {
                context.accepted += 1;
                context.reply = Reply::Accepted(true);
                Handled
            }
            RoundEvent::Begin => context.start(),
            _ => Super,
        }
    }

    #[state(superstate = "any")]
    fn running(context: &mut Cover, event: &RoundEvent) -> Outcome<State> {
        match event {
            RoundEvent::Accept(_) => {
                context.accepted += 1;
                context.reply = Reply::Accepted(false);
                Handled
            }
            RoundEvent::Begin => {
                context.reply = Reply::Began(None);
                Handled
            }
            RoundEvent::Next(owner) if context.owns(*owner) => context.step(),
            RoundEvent::Leave(owner) if context.owns(*owner) => context.leave(),
            RoundEvent::End(owner) if context.owns(*owner) => {
                context.reply = Reply::Ended(Some(std::mem::take(&mut context.late)));
                Handled
            }
            RoundEvent::Abandon(owner) if context.owns(*owner) => context.abandon(),
            _ => Super,
        }
    }

    #[state(superstate = "any")]
    fn ending(context: &mut Cover, event: &RoundEvent) -> Outcome<State> {
        match event {
            RoundEvent::Accept(items) => {
                context.accepted += 1;
                context.late.extend_from_slice(items);
                context.reply = Reply::Accepted(true);
                Handled
            }
            RoundEvent::Begin => context.start(),
            RoundEvent::Next(owner) if context.owns(*owner) => context.step(),
            RoundEvent::Leave(owner) if context.owns(*owner) => context.leave(),
            RoundEvent::End(owner) if context.owns(*owner) => {
                context.reply = Reply::Ended(Some(std::mem::take(&mut context.late)));
                Transition(State::idle())
            }
            RoundEvent::Abandon(owner) if context.owns(*owner) => context.abandon(),
            _ => Super,
        }
    }
}

fn phase_of(state: &State) -> Phase {
    match state {
        State::Idle {} => Phase::Idle,
        State::Running {} => Phase::Running,
        State::Ending {} => Phase::Ending,
    }
}

#[derive(Default)]
pub(crate) struct UploadRound {
    cover: Cover,
}

impl UploadRound {
    pub(crate) fn is_idle(&self) -> bool {
        self.cover.phase == Phase::Idle
    }

    fn send(&mut self, event: RoundEvent) -> Reply {
        self.cover.reply = Reply::None;
        let mut machine = RoundFlow.uninitialized_state_machine().init_with_context(&mut self.cover);
        machine.handle_with_context(&RoundEvent::Restore, &mut self.cover);
        machine.handle_with_context(&event, &mut self.cover);
        self.cover.phase = phase_of(machine.state());
        std::mem::take(&mut self.cover.reply)
    }

    pub(crate) fn accept(&mut self, items: &[AcceptedUpload]) -> bool {
        matches!(self.send(RoundEvent::Accept(items.to_vec())), Reply::Accepted(true))
    }

    pub(crate) fn begin(&mut self) -> Option<u32> {
        match self.send(RoundEvent::Begin) {
            Reply::Began(owner) => owner,
            _ => None,
        }
    }

    pub(crate) fn next(&mut self, owner: u32) -> RoundStep {
        self.step(RoundEvent::Next(owner))
    }

    pub(crate) fn leave(&mut self, owner: u32) -> RoundStep {
        self.step(RoundEvent::Leave(owner))
    }

    fn step(&mut self, event: RoundEvent) -> RoundStep {
        match self.send(event) {
            Reply::Step(step) => step,
            _ => RoundStep::Stale,
        }
    }

    pub(crate) fn end(&mut self, owner: u32) -> Option<Vec<AcceptedUpload>> {
        match self.send(RoundEvent::End(owner)) {
            Reply::Ended(late) => late,
            _ => None,
        }
    }

    pub(crate) fn abandon(&mut self, owner: u32) {
        self.send(RoundEvent::Abandon(owner));
    }
}

impl Db {
    pub(crate) fn round_accept(&self, items: &[AcceptedUpload]) -> bool {
        locked(&self.shared.upload_round).accept(items)
    }

    pub fn round_begin(&self) -> Option<u32> {
        locked(&self.shared.upload_round).begin()
    }

    pub fn round_next(&self, owner: u32) -> RoundStep {
        locked(&self.shared.upload_round).next(owner)
    }

    pub fn round_leave(&self, owner: u32) -> RoundStep {
        locked(&self.shared.upload_round).leave(owner)
    }

    pub fn round_end(&self, owner: u32) -> Option<Vec<AcceptedUpload>> {
        locked(&self.shared.upload_round).end(owner)
    }

    pub fn round_abandon(&self, owner: u32) {
        locked(&self.shared.upload_round).abandon(owner);
    }
}
