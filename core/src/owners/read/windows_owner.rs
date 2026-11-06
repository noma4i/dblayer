/// One machine of a window.
struct Window {
    machine: InitializedStateMachine<Facts>,
}

/// What a window knows of its owner when it opens.
struct WindowBasis {
    epoch: Epoch,
    online: Option<bool>,
    base_generation: u64,
    generations: BTreeMap<String, u64>,
}

impl Window {
    fn new(model: String, basis: WindowBasis, max_pages: u32, members_complete: bool, ids: (&mut RequestId, &mut u32)) -> Self {
        let WindowBasis { epoch, online, base_generation, generations } = basis;
        let facts = Facts {
            dependency_models: BTreeSet::from([model]),
            epoch,
            online,
            base_generation,
            generations,
            interest: BTreeSet::new(),
            reload: BTreeSet::new(),
            dropped: false,
            reconnected: false,
            waiters: BTreeMap::new(),
            fresh: false,
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
            fx: RefCell::new(Vec::new()),
        };
        let mut ctx = Ctx { next_request: ids.0, next_attempt: ids.1 };
        Self { machine: facts.uninitialized_state_machine().init_with_context(&mut ctx) }
    }

    fn handle(&mut self, event: Ev, ids: (&mut RequestId, &mut u32)) -> Vec<Emit> {
        let previous = self.request_id();
        let mut ctx = Ctx { next_request: ids.0, next_attempt: ids.1 };
        self.machine.handle_with_context(&event, &mut ctx);
        let mut effects = self.machine.fx.take();
        if let Some(request) = previous.filter(|request| Some(*request) != self.request_id()) {
            effects.insert(0, Emit::Unpin { request });
        }
        effects
    }

    fn request_id(&self) -> Option<RequestId> {
        match self.machine.state() {
            WinState::Loading { load } => Some(load.request),
            WinState::Landing { request, .. } => Some(*request),
            _ => None,
        }
    }

    fn stage(&self) -> WindowStage {
        match self.machine.state() {
            WinState::Idle {} => WindowStage::Idle,
            WinState::Loading { .. } => WindowStage::Loading,
            WinState::Landing { .. } => WindowStage::Landing,
            WinState::Waiting { .. } => WindowStage::Waiting,
            WinState::Ended {} => WindowStage::Ended,
        }
    }

    fn owns(&self, id: RequestId) -> bool {
        match self.machine.state() {
            WinState::Loading { load } => load.request == id,
            WinState::Landing { request, .. } => *request == id,
            _ => false,
        }
    }
}

/// How many pages a key may land at most: the `max_pages` of its operation in the `.dbl`.
pub type MaxPages = Arc<dyn Fn(&WindowKey) -> u32 + Send + Sync>;

/// Whether the `.dbl` declares `members_complete` for the relation of a key.
pub type MembersComplete = Arc<dyn Fn(&WindowKey) -> bool + Send + Sync>;

/// Whether the `.dbl` declares `refresh: foreground` for the relation of a key.
pub type OnForeground = Arc<dyn Fn(&WindowKey) -> bool + Send + Sync>;

/// The state of the owner: the fields the machines read (`online`, `generation[m]`), the keys declared `preload` and the machines.
pub struct WindowsState {
    epoch: Epoch,
    online: Option<bool>,
    generations: BTreeMap<String, u64>,
    base_generation: u64,
    preload: Vec<WindowKey>,
    sources: BTreeSet<WindowKey>,
    next_request: RequestId,
    next_attempt: u32,
    windows: BTreeMap<WindowKey, Window>,
    max_pages: MaxPages,
    members_complete: MembersComplete,
    on_foreground: OnForeground,
    foreground_seen: u64,
    peers: WindowsPeers,
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
        Self { epoch, online, generations: BTreeMap::new(), base_generation: 0, preload: Vec::new(), sources: BTreeSet::new(), next_request: 1, next_attempt: 1, windows: BTreeMap::new(), max_pages: Arc::new(|_| u32::MAX), members_complete: Arc::new(|_| false), on_foreground: Arc::new(|_| false), foreground_seen: 0, peers }
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

    pub fn with_max_pages(mut self, max_pages: MaxPages) -> Self {
        self.max_pages = max_pages;
        self
    }

    pub fn with_foreground(mut self, on_foreground: OnForeground) -> Self {
        self.on_foreground = on_foreground;
        self
    }

    pub fn stage(&self, key: &WindowKey) -> Option<WindowStage> { self.windows.get(key).map(Window::stage) }

    pub fn interest(&self, key: &WindowKey) -> Vec<InterestId> { self.windows.get(key).map(|window| window.machine.interest.iter().copied().collect()).unwrap_or_default() }

    pub fn waiters(&self, key: &WindowKey) -> Vec<InterestId> { self.windows.get(key).map(|window| window.machine.waiters.keys().copied().collect()).unwrap_or_default() }

    pub fn load_state(&self, key: &WindowKey) -> Option<WindowState> { self.windows.get(key).map(|window| window.machine.load_state()) }

    pub fn windows(&self) -> usize { self.windows.len() }

    fn ensure(&mut self, key: &WindowKey) {
        if !self.windows.contains_key(key) {
            let ids = (&mut self.next_request, &mut self.next_attempt);
            let window = Window::new(key.model.clone(), WindowBasis { epoch: self.epoch, online: self.online, base_generation: self.base_generation, generations: self.generations.clone() }, (self.max_pages)(key), (self.members_complete)(key), ids);
            self.windows.insert(key.clone(), window);
        }
    }
}

pub struct Windows;

impl Windows {
    fn on(state: &mut WindowsState, key: &WindowKey, event: Ev, out: &mut Effects<Self>) {
        let peers = state.peers;
        let Some(window) = state.windows.get_mut(key) else { return };
        let epoch = window.machine.epoch;
        for emit in window.handle(event, (&mut state.next_request, &mut state.next_attempt)) {
            match emit {
                Emit::Run(effect) => out.push(Effect::Run(WindowsEffect::Window { key: key.clone(), effect })),
                Emit::Pin { epoch, request } => out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Pin { epoch, request_id: request }))),
                Emit::Unpin { request } => out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Unpin { request_id: request }))),
                Emit::Commit { request, basis, page, number, replace_all, base_revision } => {
                    let op = StoreOp::Window { key: key.name.clone(), model: key.model.clone(), page: number, replace_all, cursor: page.cursor.clone(), roots: page.payload.roots.clone(), related: page.payload.related.clone(), total: page.payload.total, base_revision };
                    let batch = StoreBatch { author: BatchAuthor::Window, id: request, epoch, basis, rows: Vec::new(), intents: 0, credential: None, ops: vec![op] };
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

    fn by_request(state: &mut WindowsState, request: RequestId, event: Ev, out: &mut Effects<Self>) {
        let Some(key) = state.windows.iter().find(|(_, window)| window.owns(request)).map(|(key, _)| key.clone()) else { return };
        Self::on(state, &key, event, out);
    }

    fn every(state: &mut WindowsState, event: &Ev, out: &mut Effects<Self>) {
        let keys: Vec<WindowKey> = state.windows.keys().cloned().collect();
        for key in keys {
            Self::on(state, &key, event.clone(), out);
        }
    }

    fn interest_plus(state: &mut WindowsState, key: WindowKey, id: InterestId, out: &mut Effects<Self>) {
        state.ensure(&key);
        Self::on(state, &key, Ev::InterestPlus { id }, out);
    }

    fn interest_passive(state: &mut WindowsState, key: WindowKey, id: InterestId, out: &mut Effects<Self>) {
        state.ensure(&key);
        Self::on(state, &key, Ev::InterestPassive { id }, out);
    }

    fn reload_on_connect(state: &mut WindowsState, key: WindowKey, id: InterestId, out: &mut Effects<Self>) {
        state.ensure(&key);
        Self::on(state, &key, Ev::ReloadOnConnect { id }, out);
    }

    fn load(state: &mut WindowsState, key: WindowKey, id: InterestId, goal: Goal, force: bool, store_revision: i64, out: &mut Effects<Self>) {
        if key.is_empty_nodes() {
            let effect = WindowEffect::Outcome { id, outcome: WindowOutcome::Loaded { revision: store_revision, has_more: false, fetched: false } };
            out.push(Effect::Run(WindowsEffect::Window { key, effect }));
            return;
        }
        state.ensure(&key);
        Self::on(state, &key, Ev::Load { id, goal, force, store_revision }, out);
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
            WindowsInput::InterestPlus { key, id } => Self::interest_plus(state, key, id, &mut out),
            WindowsInput::InterestPassive { key, id } => Self::interest_passive(state, key, id, &mut out),
            WindowsInput::ReloadOnConnect { key, id } => Self::reload_on_connect(state, key, id, &mut out),
            WindowsInput::OfflineRefused { key, message } => Self::on(state, &key, Ev::OfflineRefused { message }, &mut out),
            WindowsInput::InterestMinus { key, id } => Self::on(state, &key, Ev::InterestMinus { id }, &mut out),
            WindowsInput::Load { key, id, goal, force, store_revision } => Self::load(state, key, id, goal, force, store_revision, &mut out),
            WindowsInput::Pinned { request_id, base_revision } => {
                if state.windows.values().any(|window| window.owns(request_id)) {
                    Self::by_request(state, request_id, Ev::Pinned { request: request_id, base_revision }, &mut out);
                } else {
                    out.push(Effect::Send(Outbound::new(state.peers.store, StoreInput::Unpin { request_id })));
                }
            }
            WindowsInput::Response { request, attempt, page } => Self::by_request(state, request, Ev::Response { request, attempt, page }, &mut out),
            WindowsInput::Failure { request, attempt, error } => Self::by_request(state, request, Ev::Failure { request, attempt, error: error.clone() }, &mut out),
            WindowsInput::Dropped { request, attempt } => Self::by_request(state, request, Ev::Failure { request, attempt, error: RequestError { message: "dropped".into(), class: ErrorClass::Network, source: None } }, &mut out),
            WindowsInput::Committed(committed) => Self::by_request(state, committed.batch, Ev::Committed { request: committed.batch, revision: committed.revision }, &mut out),
            WindowsInput::Rejected(rejected) => Self::by_request(state, rejected.batch, Ev::Rejected { request: rejected.batch, reason: rejected.reason }, &mut out),
            WindowsInput::Stale { model, keys } => {
                let named: Vec<WindowKey> = state.windows.keys().filter(|key| key.model == model && keys.names(key)).cloned().collect();
                for key in named {
                    Self::on(state, &key, Ev::Stale, &mut out);
                }
            }
            WindowsInput::RetryDue { request } => Self::by_request(state, request, Ev::RetryDue { request }, &mut out),
            WindowsInput::PreloadKeys { keys } => {
                for key in keys {
                    if !state.windows.contains_key(&key) {
                        Self::interest_plus(state, key, PRELOAD, &mut out);
                    }
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
            WindowsInput::Online(Online(online)) => {
                state.online = Some(online);
                Self::every(state, &Ev::Online(online), &mut out);
            }
            WindowsInput::EpochStarted(started) => {
                state.epoch = started.epoch;
                state.base_generation = started.generation;
                state.generations.clear();
                state.windows.clear();
                state.online = None;
                state.foreground_seen = 0;
                if started.identity.is_some() {
                    out.push(Effect::Run(WindowsEffect::SnapshotRequest { epoch: started.epoch }));
                    for key in state.preload.clone() {
                        Self::interest_plus(state, key, PRELOAD, &mut out);
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

    fn tick(now: Time) -> WindowsInput { WindowsInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> WindowsInput { WindowsInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> WindowsInput { WindowsInput::Closing }
}
