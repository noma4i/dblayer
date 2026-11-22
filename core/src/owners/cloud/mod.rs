use std::collections::BTreeSet;
use sha2::{Digest, Sha256};
pub(crate) mod runner;

use crate::runtime::{Cause, Effect, Effects, Epoch, RetryPolicy, ScheduleId, StateOwner, Time};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CloudScope {
    pub epoch: Epoch,
    pub generation: u64,
    pub domain: String,
    pub provider_fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CloudInput {
    EpochStarted(Epoch),
    EpochEnded(Epoch),
    Online(bool),
    Provider(crate::ffi::CloudAccount),
    Application { epoch: Epoch, key: Option<String> },
    InitialRead { generation: u64 },
    BootstrapDone { generation: u64 },
    Erased { token: u64 },
    EraseFailed { token: u64 },
    Stopped { generation: u64 },
    StartFailed { generation: u64 },
    Wake,
    SendDone { generation: u64, more: bool, retry: bool },
    Retry { generation: u64 },
    Relay { scope: CloudScope, event: crate::ffi::CloudRelayEvent },
    RelayDue { generation: u64, token: u64 },
    Close,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CloudEffect {
    Start(CloudScope),
    Stop { generation: u64 },
    Erase { domain: String, token: u64 },
    Ready(CloudScope),
    Bootstrap(CloudScope),
    Send(CloudScope),
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
    bootstrapping: bool,
    erasing: Option<u64>,
    stopping: BTreeSet<u64>,
    next_token: u64,
    exhausted: bool,
    closed: bool,
    retry_held: bool,
    sending: bool,
    wake: bool,
    backoff: bool,
    attempts: u32,
    relay_attempts: u32,
    relay_token: Option<u64>,
    backend: Option<String>,
    provider: Option<crate::ffi::CloudAccount>,
    application: Option<String>,
    standalone: bool,
    provider_fingerprint: String,
}

impl CloudState {
    pub(crate) fn new(epoch: Epoch) -> Self {
        Self { epoch, attached: true, online: false, available: false, desired: None, domain: None, active: None, ready: false, bootstrapping: false, erasing: None, stopping: BTreeSet::new(), next_token: 0, exhausted: false, closed: false, retry_held: false, sending: false, wake: false, backoff: false, attempts: 0, relay_attempts: 0, relay_token: None, backend: None, provider: None, application: None, standalone: false, provider_fingerprint: String::new() }
    }

    pub(crate) fn with_domain(epoch: Epoch, domain: Option<String>) -> Self {
        let mut state = Self::new(epoch);
        state.domain = domain;
        state
    }

    pub(crate) fn configured(epoch: Epoch, domain: Option<String>, backend: &crate::ffi::CloudBackend, application: &crate::ffi::CloudApplication) -> Self {
        let mut state = Self::with_domain(epoch, domain);
        state.backend = Some(match backend { crate::ffi::CloudBackend::CloudKit => "cloudkit", crate::ffi::CloudBackend::Drive => "drive" }.into());
        if let crate::ffi::CloudApplication::Standalone { namespace } = application {
            state.application = Some(namespace.clone());
            state.standalone = true;
        } else { state.attached = false; }
        state
    }

    fn account_inputs(&mut self, fx: &mut Effects<Cloud>) {
        self.retry_held = false;
        match (&self.provider, &self.application, &self.backend) {
            (Some(crate::ffi::CloudAccount::SignedOut), _, _) => { self.available = true; self.desired = None; }
            (Some(crate::ffi::CloudAccount::Available { key }), Some(app), Some(backend)) => {
                let mut hash = Sha256::new();
                for part in [backend, key, app] { hash.update((part.len() as u64).to_be_bytes()); hash.update(part.as_bytes()); }
                self.desired = Some(hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect());
                self.provider_fingerprint = Sha256::digest(key.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
                self.available = true;
            }
            _ => { self.available = false; self.stop(fx); return; }
        }
        self.reconcile(fx);
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
            self.bootstrapping = false;
            self.sending = false;
            self.wake = false;
            self.backoff = false;
            self.attempts = 0;
            fx.push(Effect::Cancel(ScheduleId(0)));
            self.relay_token = None;
            fx.push(Effect::Cancel(ScheduleId(1)));
            self.stopping.insert(scope.generation);
            fx.push(Effect::Run(CloudEffect::Stop { generation: scope.generation }));
        }
    }

    fn start_pass(&mut self, fx: &mut Effects<Cloud>) {
        if self.ready && !self.sending && !self.backoff {
            if let Some(scope) = &self.active {
                self.sending = true;
                self.wake = false;
                fx.push(Effect::Run(CloudEffect::Send(scope.clone())));
            }
        }
    }

    fn reconcile(&mut self, fx: &mut Effects<Cloud>) {
        if self.exhausted || self.erasing.is_some() || !self.attached || !self.available { return; }
        if self.domain != self.desired {
            self.stop(fx);
            if !self.stopping.is_empty() || self.retry_held { return; }
            if let Some(domain) = self.domain.clone() {
                let Some(token) = self.token(fx) else { return; };
                self.erasing = Some(token);
                fx.push(Effect::Run(CloudEffect::Erase { domain, token }));
                return;
            }
            self.domain.clone_from(&self.desired);
        }
        if self.online && self.active.is_none() && self.stopping.is_empty() && !self.retry_held {
            if let Some(domain) = self.domain.clone() {
                let Some(generation) = self.token(fx) else { return; };
                let scope = CloudScope { epoch: self.epoch, generation, domain, provider_fingerprint: self.provider_fingerprint.clone() };
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
            match input {
                CloudInput::Stopped { generation } if state.stopping.remove(&generation) => {}
                CloudInput::Erased { token } | CloudInput::EraseFailed { token } if state.erasing == Some(token) => { state.erasing = None; }
                _ => return fx,
            }
            if state.stopping.is_empty() && state.erasing.is_none() { fx.push(Effect::Run(CloudEffect::Drained)); }
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
                if !state.standalone { state.application = None; }
                state.account_inputs(&mut fx);
            }
            CloudInput::EpochEnded(epoch) if epoch == state.epoch && state.attached => {
                state.stop(&mut fx);
                state.attached = false;
                state.available = false;
                state.desired = None;
                if !state.standalone { state.application = None; }
            }
            CloudInput::Online(online) => {
                state.retry_held = false;
                state.online = online;
                if !online { state.stop(&mut fx); }
                state.reconcile(&mut fx);
            }
            CloudInput::Provider(provider) => { state.provider = Some(provider); state.account_inputs(&mut fx); }
            CloudInput::Application { epoch, key } if epoch == state.epoch && state.attached && !state.standalone => {
                state.application = key;
                state.account_inputs(&mut fx);
            }
            CloudInput::InitialRead { generation } => {
                if let Some(scope) = &state.active {
                    if scope.generation == generation && !state.ready && !state.bootstrapping {
                        state.bootstrapping = true;
                        fx.push(Effect::Run(CloudEffect::Bootstrap(scope.clone())));
                    }
                }
            }
            CloudInput::BootstrapDone { generation } => {
                if let Some(scope) = &state.active {
                    if scope.generation == generation && state.bootstrapping {
                        state.bootstrapping = false;
                        state.ready = true;
                        fx.push(Effect::Run(CloudEffect::Ready(scope.clone())));
                        state.start_pass(&mut fx);
                    }
                }
            }
            CloudInput::Erased { token } if state.erasing == Some(token) => {
                state.erasing = None;
                state.domain = None;
                state.reconcile(&mut fx);
            }
            CloudInput::EraseFailed { token } if state.erasing == Some(token) => {
                state.erasing = None;
                state.retry_held = true;
            }
            CloudInput::Stopped { generation } => {
                if state.stopping.remove(&generation) { state.reconcile(&mut fx); }
            }
            CloudInput::StartFailed { generation } if state.active.as_ref().is_some_and(|scope| scope.generation == generation) => {
                state.retry_held = true;
                state.stop(&mut fx);
            }
            CloudInput::Wake => { state.wake = true; state.start_pass(&mut fx); }
            CloudInput::SendDone { generation, more, retry } if state.sending && state.active.as_ref().is_some_and(|scope| scope.generation == generation) => {
                state.sending = false;
                if retry {
                    state.attempts = state.attempts.saturating_add(1);
                    state.backoff = true;
                    fx.push(Effect::Backoff { id: ScheduleId(0), policy: RetryPolicy::Bootstrap, attempt: state.attempts, floor: std::time::Duration::ZERO, input: CloudInput::Retry { generation } });
                } else {
                    state.attempts = 0;
                    if more || state.wake { state.start_pass(&mut fx); }
                }
            }
            CloudInput::Retry { generation } if state.backoff && state.active.as_ref().is_some_and(|scope| scope.generation == generation) => {
                state.backoff = false;
                state.start_pass(&mut fx);
            }
            CloudInput::Relay { scope, event } if state.active.as_ref() == Some(&scope) => {
                state.relay_token = None;
                let lease = match event {
                    crate::ffi::CloudRelayEvent::Suspended => { fx.push(Effect::Cancel(ScheduleId(1))); return fx; }
                    crate::ffi::CloudRelayEvent::Lease { milliseconds } => { state.relay_attempts = 0; Some(milliseconds) }
                    crate::ffi::CloudRelayEvent::Ended => { state.relay_attempts = state.relay_attempts.saturating_add(1); None }
                };
                let Some(token) = state.token(&mut fx) else { return fx; };
                state.relay_token = Some(token);
                let input = CloudInput::RelayDue { generation: scope.generation, token };
                if let Some(milliseconds) = lease {
                    fx.push(Effect::Schedule { id: ScheduleId(1), after: std::time::Duration::from_millis(milliseconds), input });
                } else {
                    fx.push(Effect::Backoff { id: ScheduleId(1), policy: RetryPolicy::Bootstrap, attempt: state.relay_attempts, floor: std::time::Duration::ZERO, input });
                }
            }
            CloudInput::RelayDue { generation, token } if state.relay_token == Some(token) && state.active.as_ref().is_some_and(|scope| scope.generation == generation) => {
                state.stop(&mut fx);
            }
            CloudInput::Close => {
                state.closed = true;
                state.stop(&mut fx);
                if state.stopping.is_empty() && state.erasing.is_none() { fx.push(Effect::Run(CloudEffect::Drained)); }
            }
            _ => {}
        }
        fx
    }

    fn epoch_started(epoch: Epoch) -> Option<Self::Input> { Some(CloudInput::EpochStarted(epoch)) }
    fn epoch_ended(epoch: Epoch, _cause: Cause) -> Self::Input { CloudInput::EpochEnded(epoch) }
    fn closing() -> Self::Input { CloudInput::Close }
}
