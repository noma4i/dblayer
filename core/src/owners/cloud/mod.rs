use std::collections::BTreeSet;

use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CloudScope {
    pub epoch: Epoch,
    pub generation: u64,
    pub domain: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CloudInput {
    EpochStarted(Epoch),
    EpochEnded(Epoch),
    Online(bool),
    Account { epoch: Epoch, domain: Option<String> },
    AccountUnavailable { epoch: Epoch },
    InitialRead { generation: u64 },
    Erased { token: u64 },
    Stopped { generation: u64 },
    Close,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CloudEffect {
    Start(CloudScope),
    Stop { generation: u64 },
    Erase { domain: String, token: u64 },
    Ready(CloudScope),
    TokenExhausted,
    Drained,
}

#[derive(Debug)]
pub(crate) struct CloudState {
    epoch: Epoch,
    attached: bool,
    online: bool,
    available: bool,
    desired: Option<String>,
    domain: Option<String>,
    active: Option<CloudScope>,
    ready: bool,
    erasing: Option<u64>,
    stopping: BTreeSet<u64>,
    next_token: u64,
    exhausted: bool,
    closed: bool,
}

impl CloudState {
    pub(crate) fn new(epoch: Epoch) -> Self {
        Self { epoch, attached: true, online: false, available: false, desired: None, domain: None, active: None, ready: false, erasing: None, stopping: BTreeSet::new(), next_token: 0, exhausted: false, closed: false }
    }

    fn token(&mut self, fx: &mut Effects<Cloud>) -> Option<u64> {
        if let Some(token) = self.next_token.checked_add(1) {
            self.next_token = token;
            return Some(token);
        }
        self.exhausted = true;
        self.stop(fx);
        fx.push(Effect::Run(CloudEffect::TokenExhausted));
        None
    }

    fn stop(&mut self, fx: &mut Effects<Cloud>) {
        if let Some(scope) = self.active.take() {
            self.ready = false;
            self.stopping.insert(scope.generation);
            fx.push(Effect::Run(CloudEffect::Stop { generation: scope.generation }));
        }
    }

    fn reconcile(&mut self, fx: &mut Effects<Cloud>) {
        if self.exhausted || self.erasing.is_some() || !self.attached || !self.available { return; }
        if self.domain != self.desired {
            self.stop(fx);
            if let Some(domain) = self.domain.clone() {
                let Some(token) = self.token(fx) else { return; };
                self.domain = None;
                self.erasing = Some(token);
                fx.push(Effect::Run(CloudEffect::Erase { domain, token }));
                return;
            }
            self.domain.clone_from(&self.desired);
        }
        if self.online && self.active.is_none() {
            if let Some(domain) = self.domain.clone() {
                let Some(generation) = self.token(fx) else { return; };
                let scope = CloudScope { epoch: self.epoch, generation, domain };
                self.active = Some(scope.clone());
                fx.push(Effect::Run(CloudEffect::Start(scope)));
            }
        }
    }
}

pub(crate) struct Cloud;

impl StateOwner for Cloud {
    const EPOCHED: bool = false;
    type State = CloudState;
    type Input = CloudInput;
    type Effect = CloudEffect;

    fn step(state: &mut CloudState, input: CloudInput, _now: Time) -> Effects<Self> {
        let mut fx = Vec::new();
        if state.closed {
            if let CloudInput::Stopped { generation } = input {
                if state.stopping.remove(&generation) && state.stopping.is_empty() {
                    fx.push(Effect::Run(CloudEffect::Drained));
                }
            }
            return fx;
        }
        if state.exhausted && !matches!(input, CloudInput::Close | CloudInput::Stopped { .. }) { return fx; }
        match input {
            CloudInput::EpochStarted(epoch) if epoch > state.epoch => {
                state.stop(&mut fx);
                state.epoch = epoch;
                state.attached = true;
                state.available = false;
                state.desired = None;
                state.domain = None;
            }
            CloudInput::EpochEnded(epoch) if epoch == state.epoch && state.attached => {
                state.stop(&mut fx);
                state.attached = false;
                state.available = false;
                state.desired = None;
                state.domain = None;
            }
            CloudInput::Online(online) => {
                state.online = online;
                if !online { state.stop(&mut fx); }
                state.reconcile(&mut fx);
            }
            CloudInput::Account { epoch, domain } if epoch == state.epoch && state.attached => {
                state.available = true;
                state.desired = domain;
                state.reconcile(&mut fx);
            }
            CloudInput::AccountUnavailable { epoch } if epoch == state.epoch && state.attached => {
                state.available = false;
                state.stop(&mut fx);
            }
            CloudInput::InitialRead { generation } => {
                if let Some(scope) = &state.active {
                    if scope.generation == generation && !state.ready {
                        state.ready = true;
                        fx.push(Effect::Run(CloudEffect::Ready(scope.clone())));
                    }
                }
            }
            CloudInput::Erased { token } if state.erasing == Some(token) => {
                state.erasing = None;
                state.reconcile(&mut fx);
            }
            CloudInput::Stopped { generation } => { state.stopping.remove(&generation); }
            CloudInput::Close => {
                state.closed = true;
                state.stop(&mut fx);
                if state.stopping.is_empty() { fx.push(Effect::Run(CloudEffect::Drained)); }
            }
            _ => {}
        }
        fx
    }

    fn epoch_started(epoch: Epoch) -> Option<Self::Input> { Some(CloudInput::EpochStarted(epoch)) }
    fn epoch_ended(epoch: Epoch, _cause: Cause) -> Self::Input { CloudInput::EpochEnded(epoch) }
    fn closing() -> Self::Input { CloudInput::Close }
}
