use super::*;

/// One machine of a window. While `opening` is set the saved window is read in the read pool and the inputs of the key wait in `held`.
pub(super) struct Window {
    pub(super) machine: InitializedStateMachine<Facts>,
    pub(super) opening: Option<Opening>,
}

pub(super) struct Opening {
    pub(super) read_id: u64,
    pub(super) basis: u64,
    pub(super) held: Vec<Ev>,
}

/// What a window knows of its owner when it opens.
pub(super) struct WindowBasis {
    pub(super) epoch: Epoch,
    pub(super) online: Option<bool>,
    pub(super) online_seq: u64,
    pub(super) saving: bool,
    pub(super) base_generation: u64,
    pub(super) generations: BTreeMap<String, u64>,
}

impl Window {
    pub(super) fn new(model: String, basis: WindowBasis, (max_pages, members_complete, lifetime, media): (u32, bool, (i64, i64), bool), read_id: Option<u64>, ids: (&mut RequestId, &mut u32)) -> Self {
        let WindowBasis { epoch, online, online_seq, saving, base_generation, generations } = basis;
        let opening = read_id.map(|read_id| Opening { read_id, basis: generations.get(&model).copied().unwrap_or(base_generation), held: Vec::new() });
        let facts = Facts {
            dependency_models: BTreeSet::from([model]),
            epoch,
            online,
            online_seq,
            load_seq: std::cell::Cell::new(0),
            base_generation,
            generations,
            interest: BTreeSet::new(),
            reload: BTreeSet::new(),
            dropped: false,
            reconnected: false,
            waiters: BTreeMap::new(),
            fresh: false,
            saving,
            landed_at: 0,
            fresh_until: 0,
            lifetime,
            answered_at: 0,
            cursor: None,
            page: 0,
            page_count: 0,
            max_pages,
            members_complete,
            cursor_reset_used: false,
            replace_all: false,
            reread_due: false,
            has_rows: false,
            phase: Phase::Idle,
            settled: false,
            error: None,
            landed_revision: 0,
            store_seen: 0,
            request_base: std::cell::Cell::new(0),
            stale_marked: std::cell::Cell::new(false),
            stale_owed: std::cell::Cell::new(false),
            media,
            marker: None,
            members: 0,
            landing_marker: None,
            fx: RefCell::new(Vec::new()),
        };
        let mut ctx = Ctx { next_request: ids.0, next_attempt: ids.1 };
        Self { machine: facts.uninitialized_state_machine().init_with_context(&mut ctx), opening }
    }

    pub(super) fn handle(&mut self, event: Ev, ids: (&mut RequestId, &mut u32)) -> Vec<Emit> {
        if let Some(opening) = &mut self.opening {
            match event {
                Ev::EpochEnded | Ev::Closing => {
                    let held = std::mem::take(&mut opening.held);
                    self.opening = None;
                    let mut effects: Vec<Emit> = held.iter().filter_map(|held| if let Ev::Load { id, .. } = held { Some(Emit::Run(WindowEffect::Outcome { id: *id, outcome: WindowOutcome::Interrupted })) } else { None }).collect();
                    effects.extend(self.run(event, ids));
                    return effects;
                }
                Ev::StoreSeen { .. } | Ev::SourceChanged { .. } => return self.run(event, ids),
                Ev::Foreground => return Vec::new(),
                held => {
                    opening.held.push(held);
                    return Vec::new();
                }
            }
        }
        self.run(event, ids)
    }

    /// The saved window arrived: the machine takes it (a window of another generation or one that a reset touched is none), then the held inputs run in the order they came.
    pub(super) fn opened(&mut self, saved: Option<SavedWindow>, ids: (&mut RequestId, &mut u32)) -> Vec<Emit> {
        let Some(opening) = self.opening.take() else { return Vec::new() };
        let reset = opening.held.iter().any(|held| matches!(held, Ev::ModelsReset { models, .. } if self.machine.depends_on(models)));
        let saved = saved.filter(|saved| saved.generation == opening.basis && !reset);
        let (next_request, next_attempt) = ids;
        let mut effects = self.run(Ev::Saved(saved), (&mut *next_request, &mut *next_attempt));
        for held in opening.held {
            effects.extend(self.run(held, (&mut *next_request, &mut *next_attempt)));
        }
        effects
    }

    pub(super) fn run(&mut self, event: Ev, ids: (&mut RequestId, &mut u32)) -> Vec<Emit> {
        let previous = self.request_id();
        let mut ctx = Ctx { next_request: ids.0, next_attempt: ids.1 };
        self.machine.handle_with_context(&event, &mut ctx);
        let mut effects = self.machine.fx.take();
        if let Some(request) = previous.filter(|request| Some(*request) != self.request_id()) {
            effects.insert(0, Emit::Unpin { request });
        }
        effects
    }

    pub(super) fn request_id(&self) -> Option<RequestId> {
        match self.machine.state() {
            WinState::Loading { load } => Some(load.request),
            WinState::Landing { landing } => Some(landing.request),
            _ => None,
        }
    }

    /// The interest and the waiters the inputs held while opening already changed.
    pub(super) fn held_ids(&self, opening: &Opening) -> (BTreeSet<InterestId>, BTreeSet<InterestId>) {
        let (mut interest, mut waiters) = (BTreeSet::new(), BTreeSet::new());
        for held in &opening.held {
            match held {
                Ev::InterestPlus { id, .. } | Ev::InterestPassive { id } => { interest.insert(*id); }
                Ev::Load { id, .. } => { waiters.insert(*id); }
                Ev::InterestMinus { id } => { interest.remove(id); waiters.remove(id); }
                _ => {}
            }
        }
        (interest, waiters)
    }

    pub(super) fn interest(&self) -> Vec<InterestId> {
        match &self.opening {
            Some(opening) => self.held_ids(opening).0.into_iter().collect(),
            None => self.machine.interest.iter().copied().collect(),
        }
    }

    pub(super) fn waiters(&self) -> Vec<InterestId> {
        match &self.opening {
            Some(opening) => self.held_ids(opening).1.into_iter().collect(),
            None => self.machine.waiters.keys().copied().collect(),
        }
    }

    pub(super) fn stage(&self) -> WindowStage {
        if self.opening.is_some() {
            return WindowStage::Opening;
        }
        match self.machine.state() {
            WinState::Idle {} => WindowStage::Idle,
            WinState::Loading { .. } => WindowStage::Loading,
            WinState::Landing { .. } => WindowStage::Landing,
            WinState::Waiting { .. } => WindowStage::Waiting,
            WinState::Ended {} => WindowStage::Ended,
        }
    }

    pub(super) fn reading(&self, read_id: u64) -> bool { self.opening.as_ref().is_some_and(|opening| opening.read_id == read_id) }

    pub(super) fn owns(&self, id: RequestId) -> bool {
        match self.machine.state() {
            WinState::Loading { load } => load.request == id,
            WinState::Landing { landing } => landing.request == id,
            _ => false,
        }
    }
}

/// How many pages a key may land at most: the `max_pages` of its operation in the `.dbl`.
pub type MaxPages = Arc<dyn Fn(&WindowKey) -> u32 + Send + Sync>;

/// `expires_in` and `empty_in` of the relation of a key, in milliseconds.
pub type Lifetimes = Arc<dyn Fn(&WindowKey) -> (i64, i64) + Send + Sync>;

/// Whether the `.dbl` declares `members_complete` for the relation of a key.
pub type MembersComplete = Arc<dyn Fn(&WindowKey) -> bool + Send + Sync>;

/// Whether the `.dbl` declares `media` (a marker and a tail) for the relation of a key.
pub type HasMedia = Arc<dyn Fn(&WindowKey) -> bool + Send + Sync>;

/// Whether the `.dbl` declares `refresh: foreground` for the relation of a key.
pub type OnForeground = Arc<dyn Fn(&WindowKey) -> bool + Send + Sync>;

/// The state of the owner: the fields the machines read (`online`, `generation[m]`), the keys declared `preload` and the machines.
pub struct WindowsState {
    pub(super) epoch: Epoch,
    pub(super) online: Option<bool>,
    pub(super) online_seq: u64,
    pub(super) saving: bool,
    pub(super) generations: BTreeMap<String, u64>,
    pub(super) base_generation: u64,
    pub(super) preload: Vec<WindowKey>,
    pub(super) sources: BTreeSet<WindowKey>,
    pub(super) next_request: RequestId,
    pub(super) next_attempt: u32,
    pub(super) windows: BTreeMap<WindowKey, Window>,
    pub(super) max_pages: MaxPages,
    pub(super) members_complete: MembersComplete,
    pub(super) lifetimes: Lifetimes,
    pub(super) media: HasMedia,
    pub(super) next_read: u64,
    pub(super) on_foreground: OnForeground,
    pub(super) foreground_seen: u64,
    pub(super) peers: WindowsPeers,
}

/// The owners `Windows` sends inputs to.
#[derive(Clone, Copy, Debug)]
pub struct WindowsPeers {
    pub store: Address<StoreWriter>,
    pub deliveries: Address<Deliveries>,
}

impl WindowsState {
    /// `online` is the value of the first addressed `Online`, `None` before it.
    pub fn new(epoch: Epoch, online: Option<bool>, peers: WindowsPeers) -> Self {
        Self { epoch, online, online_seq: 0, saving: false, generations: BTreeMap::new(), base_generation: 0, preload: Vec::new(), sources: BTreeSet::new(), next_request: 1, next_attempt: 1, windows: BTreeMap::new(), max_pages: Arc::new(|_| u32::MAX), members_complete: Arc::new(|_| false), lifetimes: Arc::new(|_| (300_000, 300_000)), media: Arc::new(|_| false), next_read: 1, on_foreground: Arc::new(|_| false), foreground_seen: 0, peers }
    }

    /// Keys that `EpochStarted` loads with `Interest+(PRELOAD)`.
    pub fn with_preload(mut self, keys: Vec<WindowKey>) -> Self {
        self.preload = keys;
        self
    }

    /// Keys whose rows name other keys to preload: a Store commit of their model asks `DerivePreload`.
    pub fn with_sources(mut self, keys: Vec<WindowKey>) -> Self {
        self.sources = keys.into_iter().collect();
        self
    }

    pub fn with_members_complete(mut self, members_complete: MembersComplete) -> Self {
        self.members_complete = members_complete;
        self
    }

    pub fn with_media(mut self, media: HasMedia) -> Self {
        self.media = media;
        self
    }

    pub fn with_lifetimes(mut self, lifetimes: Lifetimes) -> Self {
        self.lifetimes = lifetimes;
        self
    }

    pub fn with_max_pages(mut self, max_pages: MaxPages) -> Self {
        self.max_pages = max_pages;
        self
    }

    pub fn with_foreground(mut self, on_foreground: OnForeground) -> Self {
        self.on_foreground = on_foreground;
        self
    }

    pub fn stage(&self, key: &WindowKey) -> Option<WindowStage> { self.windows.get(key).map(Window::stage) }

    pub fn interest(&self, key: &WindowKey) -> Vec<InterestId> { self.windows.get(key).map(|window| window.interest()).unwrap_or_default() }

    pub fn waiters(&self, key: &WindowKey) -> Vec<InterestId> { self.windows.get(key).map(Window::waiters).unwrap_or_default() }

    pub fn load_state(&self, key: &WindowKey) -> Option<WindowState> { self.windows.get(key).map(|window| window.machine.load_state()) }

    pub fn windows(&self) -> usize { self.windows.len() }

    /// The read of the saved window the key waits for.
    pub fn reading(&self, key: &WindowKey) -> Option<u64> { self.windows.get(key).and_then(|window| window.opening.as_ref().map(|opening| opening.read_id)) }

    /// A key without a machine gets one in `Opening` and asks for its saved window.
    pub(super) fn ensure(&mut self, key: &WindowKey, out: &mut Effects<Windows>) {
        if !self.windows.contains_key(key) {
            let read_id = self.next_read;
            self.next_read += 1;
            self.create(key, Some(read_id));
            out.push(Effect::Run(WindowsEffect::ReadWindow { key: key.clone(), read_id }));
        }
    }

    /// A machine of the preload interest starts not fresh: there is no saved window to read.
    pub(super) fn create(&mut self, key: &WindowKey, read_id: Option<u64>) {
        let ids = (&mut self.next_request, &mut self.next_attempt);
        let window = Window::new(key.model.clone(), WindowBasis { epoch: self.epoch, online: self.online, online_seq: self.online_seq, saving: self.saving, base_generation: self.base_generation, generations: self.generations.clone() }, ((self.max_pages)(key), (self.members_complete)(key), (self.lifetimes)(key), (self.media)(key)), read_id, ids);
        self.windows.insert(key.clone(), window);
    }
}

pub struct Windows;

impl Windows {
    pub(super) fn on(state: &mut WindowsState, key: &WindowKey, event: Ev, out: &mut Effects<Self>) {
        let Some(window) = state.windows.get_mut(key) else { return };
        let emits = window.handle(event, (&mut state.next_request, &mut state.next_attempt));
        Self::emitted(state, key, emits, out);
    }

    pub(super) fn emitted(state: &mut WindowsState, key: &WindowKey, emits: Vec<Emit>, out: &mut Effects<Self>) {
        let peers = state.peers;
        let Some(window) = state.windows.get(key) else { return };
        let epoch = window.machine.epoch;
        for emit in emits {
            match emit {
                Emit::Run(effect) => out.push(Effect::Run(WindowsEffect::Window { key: key.clone(), effect })),
                Emit::Pin { epoch, request } => out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Pin { epoch, request_id: request }))),
                Emit::Unpin { request } => out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Unpin { request_id: request }))),
                Emit::Commit { request, basis, page, number, replace_all, base_revision, marker, keep_rows } => {
                    let mut ops = Vec::new();
                    if !keep_rows {
                        ops.push(StoreOp::Window { key: key.name.clone(), model: key.model.clone(), page: number, replace_all, cursor: page.cursor.clone(), roots: page.payload.roots.clone(), related: page.payload.related.clone(), total: page.payload.total, base_revision });
                    }
                    if let Some(marker) = marker {
                        ops.push(StoreOp::WindowMarker { key: key.name.clone(), model: key.model.clone(), marker });
                    }
                    let batch = StoreBatch { author: BatchAuthor::Window, id: request, epoch, basis, rows: Vec::new(), intents: 0, credential: None, ops };
                    out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Commit(batch))));
                }
                Emit::Retry { request, attempt, class } => {
                    let policy = if class == ErrorClass::Network { RetryPolicy::QueryNetwork } else { RetryPolicy::QueryServer };
                    let after: Duration = policy.backoff().delay(attempt - 1);
                    out.push(Effect::Schedule { id: ScheduleId(request), after, input: WindowsInput::RetryDue { request } });
                }
                Emit::CancelRetry { request } => out.push(Effect::Cancel(ScheduleId(request))),
                Emit::Announce { state, streams } => {
                    for stream in streams {
                        out.push(Effect::Send(Outbound::new(peers.deliveries, DeliveryInput::WindowState { stream, state: state.clone() })));
                    }
                }
            }
        }
    }

    pub(super) fn by_request(state: &mut WindowsState, request: RequestId, event: Ev, out: &mut Effects<Self>) {
        let Some(key) = state.windows.iter().find(|(_, window)| window.owns(request)).map(|(key, _)| key.clone()) else { return };
        Self::on(state, &key, event, out);
    }

    pub(super) fn every(state: &mut WindowsState, event: &Ev, out: &mut Effects<Self>) {
        let keys: Vec<WindowKey> = state.windows.keys().cloned().collect();
        for key in keys {
            Self::on(state, &key, event.clone(), out);
        }
    }

    pub(super) fn interest_plus(state: &mut WindowsState, key: WindowKey, (id, now): (InterestId, i64), out: &mut Effects<Self>) {
        state.ensure(&key, out);
        Self::on(state, &key, Ev::InterestPlus { id, now }, out);
    }

    pub(super) fn interest_preload(state: &mut WindowsState, key: WindowKey, now: i64, out: &mut Effects<Self>) {
        match state.windows.get(&key) {
            None => state.create(&key, None),
            Some(window) if window.interest().contains(&PRELOAD) => return,
            Some(_) => {}
        }
        Self::on(state, &key, Ev::InterestPlus { id: PRELOAD, now }, out);
    }

    pub(super) fn interest_passive(state: &mut WindowsState, key: WindowKey, id: InterestId, out: &mut Effects<Self>) {
        state.ensure(&key, out);
        Self::on(state, &key, Ev::InterestPassive { id }, out);
    }

    pub(super) fn reload_on_connect(state: &mut WindowsState, key: WindowKey, id: InterestId, out: &mut Effects<Self>) {
        state.ensure(&key, out);
        Self::on(state, &key, Ev::ReloadOnConnect { id }, out);
    }

    pub(super) fn load(state: &mut WindowsState, key: WindowKey, (id, now): (InterestId, i64), (goal, force, store_revision): (Goal, bool, i64), out: &mut Effects<Self>) {
        if key.is_empty_nodes() {
            let effect = WindowEffect::Outcome { id, outcome: WindowOutcome::Loaded { revision: store_revision, has_more: false, fetched: false } };
            out.push(Effect::Run(WindowsEffect::Window { key, effect }));
            return;
        }
        state.ensure(&key, out);
        Self::on(state, &key, Ev::Load { id, goal, force, store_revision, now }, out);
    }
}

impl StateOwner for Windows {
    const EPOCHED: bool = true;

    type State = WindowsState;
    type Input = WindowsInput;
    type Effect = WindowsEffect;

    fn step(state: &mut WindowsState, input: WindowsInput) -> Effects<Self> {
        let mut out: Effects<Self> = Vec::new();
        match input {
            WindowsInput::InterestPlus { key, id, now } => Self::interest_plus(state, key, (id, now), &mut out),
            WindowsInput::InterestPassive { key, id } => Self::interest_passive(state, key, id, &mut out),
            WindowsInput::ReloadOnConnect { key, id } => Self::reload_on_connect(state, key, id, &mut out),
            WindowsInput::OfflineRefused { key, message } => Self::on(state, &key, Ev::OfflineRefused { message }, &mut out),
            WindowsInput::InterestMinus { key, id } => Self::on(state, &key, Ev::InterestMinus { id }, &mut out),
            WindowsInput::Load { key, id, goal, force, store_revision, now } => Self::load(state, key, (id, now), (goal, force, store_revision), &mut out),
            WindowsInput::Pinned { request_id, base_revision } => {
                if state.windows.values().any(|window| window.owns(request_id)) {
                    Self::by_request(state, request_id, Ev::Pinned { request: request_id, base_revision }, &mut out);
                } else {
                    out.push(Effect::Send(Outbound::new(state.peers.store, StoreInput::Unpin { request_id })));
                }
            }
            WindowsInput::Response { request, attempt, page, now } => Self::by_request(state, request, Ev::Response { request, attempt, page, now }, &mut out),
            WindowsInput::Failure { request, attempt, error } => Self::by_request(state, request, Ev::Failure { request, attempt, error: error.clone() }, &mut out),
            WindowsInput::Dropped { request, attempt } => Self::by_request(state, request, Ev::Failure { request, attempt, error: RequestError { message: "dropped".into(), class: ErrorClass::Network, source: None } }, &mut out),
            WindowsInput::Committed(committed) => Self::by_request(state, committed.batch, Ev::Committed { request: committed.batch, revision: committed.revision }, &mut out),
            WindowsInput::Rejected(rejected) => Self::by_request(state, rejected.batch, Ev::Rejected { request: rejected.batch, reason: rejected.reason }, &mut out),
            WindowsInput::Stale { model, keys, cause } => {
                let named: Vec<WindowKey> = state.windows.keys().filter(|key| key.model == model && keys.names(key)).cloned().collect();
                for key in named {
                    Self::on(state, &key, Ev::Stale { cause }, &mut out);
                }
            }
            WindowsInput::RetryDue { request } => Self::by_request(state, request, Ev::RetryDue { request }, &mut out),
            WindowsInput::PreloadKeys { keys, now } => {
                let dropped: Vec<WindowKey> = state.windows.iter().filter(|(key, window)| !keys.contains(key) && !state.preload.contains(key) && window.interest().contains(&PRELOAD)).map(|(key, _)| key.clone()).collect();
                for key in dropped {
                    Self::on(state, &key, Ev::InterestMinus { id: PRELOAD }, &mut out);
                }
                for key in keys {
                    Self::interest_preload(state, key, now, &mut out);
                }
            }
            WindowsInput::StoreCommitted { models, revision } => {
                let keys: Vec<WindowKey> = state.windows.keys().cloned().collect();
                for key in keys {
                    Self::on(state, &key, Ev::StoreSeen { revision }, &mut out);
                }
                let sources: Vec<WindowKey> = state.sources.iter().filter(|key| models.contains(&key.model) && state.windows.contains_key(*key)).cloned().collect();
                for key in sources {
                    Self::on(state, &key, Ev::SourceChanged { revision }, &mut out);
                }
            }
            WindowsInput::Foreground(seq) if seq > state.foreground_seen => {
                state.foreground_seen = seq;
                let named: Vec<WindowKey> = state.windows.keys().filter(|key| (state.on_foreground)(key)).cloned().collect();
                for key in named {
                    Self::on(state, &key, Ev::Foreground, &mut out);
                }
            }
            WindowsInput::Foreground(_) => {}
            WindowsInput::Online { online: Online(online), online_seq, now } => {
                state.online = Some(online);
                if online {
                    state.online_seq = online_seq;
                }
                Self::every(state, &Ev::Online { online, online_seq, now }, &mut out);
            }
            WindowsInput::PowerSaving { saving } => {
                state.saving = saving;
                Self::every(state, &Ev::PowerSaving { saving }, &mut out);
            }
            WindowsInput::WindowRead { key, read_id, result } => {
                let Some(window) = state.windows.get_mut(&key).filter(|window| window.reading(read_id)) else { return out };
                let saved = result.unwrap_or(None);
                let emits = window.opened(saved, (&mut state.next_request, &mut state.next_attempt));
                Self::emitted(state, &key, emits, &mut out);
            }
            WindowsInput::TaskFailed { id } if id & REQUEST_TASK != 0 => {
                let (request, attempt) = task_request(id);
                let error = RequestError { message: "the request task failed".into(), class: ErrorClass::Permanent, source: None };
                return Self::step(state, WindowsInput::Failure { request, attempt, error });
            }
            WindowsInput::TaskFailed { id } => {
                let read_id = id & !READ_TASK;
                let Some(key) = state.windows.iter().find(|(_, window)| window.reading(read_id)).map(|(key, _)| key.clone()) else { return out };
                return Self::step(state, WindowsInput::WindowRead { key, read_id, result: Err("the read task failed".into()) });
            }
            WindowsInput::EpochStarted(started) => {
                state.epoch = started.epoch;
                state.base_generation = started.generation;
                state.generations.clear();
                state.windows.clear();
                state.online = None;
                state.online_seq = 0;
                state.saving = false;
                state.foreground_seen = 0;
                if started.identity.is_some() {
                    out.push(Effect::Run(WindowsEffect::SnapshotRequest { epoch: started.epoch }));
                    for key in state.preload.clone() {
                        Self::interest_preload(state, key, 0, &mut out);
                    }
                }
            }
            WindowsInput::EpochEnded(ended) => {
                let keys: Vec<WindowKey> = state.windows.iter().filter(|(_, window)| window.machine.epoch == ended.epoch).map(|(key, _)| key.clone()).collect();
                for key in keys {
                    Self::on(state, &key, Ev::EpochEnded, &mut out);
                }
            }
            WindowsInput::ModelsReset(reset) => {
                for model in &reset.models {
                    state.generations.insert(model.clone(), reset.generation);
                }
                Self::every(state, &Ev::ModelsReset { generation: reset.generation, models: reset.models }, &mut out);
            }
            WindowsInput::Closing => {
                Self::every(state, &Ev::Closing, &mut out);
                out.push(Effect::Run(WindowsEffect::Drained(Drained { owner: "windows" })));
            }
            WindowsInput::Tick(_) => {}
        }
        out
    }

    fn executor_failed(_executor: &str, pending: Vec<EffectId>) -> Vec<WindowsInput> { pending.into_iter().map(|id| WindowsInput::TaskFailed { id }).collect() }

    fn tick(now: Time) -> WindowsInput { WindowsInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> WindowsInput { WindowsInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> WindowsInput { WindowsInput::Closing }
}
