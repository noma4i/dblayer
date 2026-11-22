use std::collections::{BTreeSet, VecDeque};
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
    Initialize { reply: u64 },
    SetEnabled { enabled: bool, reply: u64 },
    PreferenceWritten(Result<(), String>),
    Observed { observation: u64 },
    Unobserved { observation: u64 },
    Provider { observation: u64, account: crate::ffi::CloudAccount },
    Application { epoch: Epoch, key: Option<String> },
    InitialRead { generation: u64 },
    BootstrapDone { generation: u64 },
    Erased { token: u64 },
    EraseFailed { token: u64 },
    Stopped { generation: u64 },
    StartFailed { generation: u64 },
    Wake,
    SendDone { generation: u64, more: bool, retry: bool },
    Issues { generation: u64, issues: Vec<crate::ffi::CloudIssue> },
    Retry { generation: u64 },
    Relay { scope: CloudScope, event: crate::ffi::CloudRelayEvent },
    RelayDue { generation: u64, token: u64 },
    Close,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CloudEffect {
    WriteEnabled(bool),
    Observe { observation: u64 },
    Unobserve { observation: u64 },
    Reply { id: u64, answer: Result<bool, String> },
    ReplyClosed { id: u64 },
    Start(CloudScope),
    Stop { generation: u64 },
    Erase { domain: String, token: u64, epoch: Epoch },
    Ready(CloudScope),
    Bootstrap(CloudScope),
    Send(CloudScope),
    TokenExhausted,
    Drained,
    Publish(crate::ffi::CloudStatus),
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
    issues: Vec<crate::ffi::CloudIssue>,
    issues_revision: u64,
    enabled: bool,
    initialized: bool,
    observation: Option<u64>,
    observing: bool,
    unobserving: bool,
    settings: VecDeque<(bool, u64, bool)>,
    setting: Option<(bool, u64, bool)>,
    writing: bool,
}

impl CloudState {
    pub(crate) fn new(epoch: Epoch) -> Self {
        Self { epoch, attached: true, online: false, available: false, desired: None, domain: None, active: None, ready: false, bootstrapping: false, erasing: None, stopping: BTreeSet::new(), next_token: 0, exhausted: false, closed: false, retry_held: false, sending: false, wake: false, backoff: false, attempts: 0, relay_attempts: 0, relay_token: None, backend: None, provider: None, application: None, standalone: false, provider_fingerprint: String::new(), issues: Vec::new(), issues_revision: 0, enabled: true, initialized: false, observation: None, observing: false, unobserving: false, settings: VecDeque::new(), setting: None, writing: false }
    }

    fn phase(&self) -> crate::ffi::CloudPhase {
        use crate::ffi::CloudPhase;
        if self.closed { CloudPhase::Closed } else if !self.enabled { CloudPhase::Disabled } else if self.exhausted || self.retry_held { CloudPhase::Failed } else if !self.attached || !self.available || self.desired.is_none() { CloudPhase::WaitingForAccount } else if !self.online { CloudPhase::Offline } else if self.ready { CloudPhase::Ready } else { CloudPhase::Reading }
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

    pub(crate) fn initial_enabled(&mut self, enabled: bool) { self.enabled = enabled; }

    fn settings_next(&mut self, fx: &mut Effects<Cloud>) {
        if self.closed || self.setting.is_some() { return; }
        let Some((enabled, reply, initialize)) = self.settings.pop_front() else { return; };
        self.setting = Some((enabled, reply, initialize));
        if !initialize && enabled != self.enabled {
            self.writing = true;
            fx.push(Effect::Run(CloudEffect::WriteEnabled(enabled)));
        } else { self.apply_setting(fx); }
    }

    fn apply_setting(&mut self, fx: &mut Effects<Cloud>) {
        let Some((enabled, _, _)) = self.setting else { return; };
        self.enabled = enabled;
        self.initialized = true;
        if enabled && self.observation.is_none() {
            let Some(observation) = self.token(fx) else { return; };
            self.observation = Some(observation);
            self.observing = true;
            fx.push(Effect::Run(CloudEffect::Observe { observation }));
        } else if !enabled {
            self.stop(fx);
            self.provider = None;
            self.available = false;
            self.detach_observer(fx);
        }
        self.finish_setting(fx);
    }

    fn detach_observer(&mut self, fx: &mut Effects<Cloud>) {
        if !self.stopping.is_empty() || self.observing || self.unobserving { return; }
        if let Some(observation) = self.observation {
            self.unobserving = true;
            fx.push(Effect::Run(CloudEffect::Unobserve { observation }));
        }
    }

    fn finish_setting(&mut self, fx: &mut Effects<Cloud>) {
        if self.writing || self.observing || self.unobserving || !self.stopping.is_empty() || self.erasing.is_some() { return; }
        if let Some((enabled, id, _)) = self.setting.take() {
            fx.push(Effect::Run(CloudEffect::Reply { id, answer: Ok(enabled) }));
        }
        self.reconcile(fx);
        self.settings_next(fx);
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
            if !self.issues.is_empty() { self.issues.clear(); self.issues_revision = self.issues_revision.wrapping_add(1); }
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
        if !self.enabled || !self.initialized || self.observing || self.unobserving || self.exhausted || self.erasing.is_some() || !self.attached || !self.available { return; }
        if self.domain != self.desired {
            self.stop(fx);
            if !self.stopping.is_empty() || self.retry_held { return; }
            if let Some(domain) = self.domain.clone() {
                let Some(token) = self.token(fx) else { return; };
                self.erasing = Some(token);
                fx.push(Effect::Run(CloudEffect::Erase { domain, token, epoch: self.epoch }));
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
        let phase = state.phase();
        let issues_revision = state.issues_revision;
        let mut fx = Self::transition(state, input);
        let next = state.phase();
        if phase != next || issues_revision != state.issues_revision {
            let closed = next == crate::ffi::CloudPhase::Closed;
            let publish = Effect::Run(CloudEffect::Publish(crate::ffi::CloudStatus { phase: next, issues: state.issues.clone() }));
            if closed { fx.insert(0, publish); } else { fx.push(publish); }
        }
        fx
    }

    fn epoch_started(epoch: Epoch) -> Option<Self::Input> { Some(CloudInput::EpochStarted(epoch)) }
    fn epoch_ended(epoch: Epoch, _cause: Cause) -> Self::Input { CloudInput::EpochEnded(epoch) }
    fn closing() -> Self::Input { CloudInput::Close }
}

impl Cloud {
    fn transition(state: &mut CloudState, input: CloudInput) -> Effects<Self> {
        let mut fx = Vec::new();
        if state.closed {
            match input {
                CloudInput::Stopped { generation } if state.stopping.remove(&generation) => { state.detach_observer(&mut fx); }
                CloudInput::Erased { token } | CloudInput::EraseFailed { token } if state.erasing == Some(token) => { state.erasing = None; }
                CloudInput::PreferenceWritten(_) => { state.writing = false; }
                CloudInput::Observed { observation } if state.observation == Some(observation) => {
                    state.observing = false;
                    state.detach_observer(&mut fx);
                }
                CloudInput::Unobserved { observation } if state.observation == Some(observation) => {
                    state.unobserving = false;
                    state.observation = None;
                }
                _ => return fx,
            }
            if state.stopping.is_empty() && state.erasing.is_none() && !state.writing && state.observation.is_none() { fx.push(Effect::Run(CloudEffect::Drained)); }
            return fx;
        }
        if state.exhausted && !matches!(input, CloudInput::Close | CloudInput::Stopped { .. }) { return fx; }
        match input {
            CloudInput::Initialize { reply } => {
                state.settings.push_back((state.enabled, reply, true));
                state.settings_next(&mut fx);
            }
            CloudInput::SetEnabled { enabled, reply } => {
                state.settings.push_back((enabled, reply, false));
                state.settings_next(&mut fx);
            }
            CloudInput::PreferenceWritten(result) if state.writing => {
                state.writing = false;
                match result {
                    Ok(()) => state.apply_setting(&mut fx),
                    Err(error) => {
                        if let Some((_, id, _)) = state.setting.take() { fx.push(Effect::Run(CloudEffect::Reply { id, answer: Err(error) })); }
                        state.settings_next(&mut fx);
                    }
                }
            }
            CloudInput::Observed { observation } if state.observation == Some(observation) && state.observing => {
                state.observing = false;
                state.finish_setting(&mut fx);
            }
            CloudInput::Unobserved { observation } if state.observation == Some(observation) && state.unobserving => {
                state.unobserving = false;
                state.observation = None;
                state.finish_setting(&mut fx);
            }
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
            CloudInput::Provider { observation, account } if state.enabled && state.observation == Some(observation) && !state.unobserving => { state.provider = Some(account); state.account_inputs(&mut fx); }
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
                state.finish_setting(&mut fx);
            }
            CloudInput::EraseFailed { token } if state.erasing == Some(token) => {
                state.erasing = None;
                state.retry_held = true;
                state.finish_setting(&mut fx);
            }
            CloudInput::Stopped { generation } => {
                if state.stopping.remove(&generation) {
                    if !state.enabled { state.detach_observer(&mut fx); }
                    state.finish_setting(&mut fx);
                }
            }
            CloudInput::StartFailed { generation } if state.active.as_ref().is_some_and(|scope| scope.generation == generation) => {
                state.retry_held = true;
                state.stop(&mut fx);
            }
            CloudInput::Wake => { state.wake = true; state.start_pass(&mut fx); }
            CloudInput::Issues { generation, issues } if state.active.as_ref().is_some_and(|scope| scope.generation == generation) => {
                if state.issues != issues { state.issues = issues; state.issues_revision = state.issues_revision.wrapping_add(1); }
            }
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
                if let Some((_, id, _)) = state.setting.take() { fx.push(Effect::Run(CloudEffect::ReplyClosed { id })); }
                for (_, id, _) in state.settings.drain(..) { fx.push(Effect::Run(CloudEffect::ReplyClosed { id })); }
                state.detach_observer(&mut fx);
                if state.stopping.is_empty() && state.erasing.is_none() && !state.writing && state.observation.is_none() { fx.push(Effect::Run(CloudEffect::Drained)); }
            }
            _ => {}
        }
        fx
    }
}
