use super::*;

/// One machine of a window. While `opening` is set the saved window is read in the read pool and the inputs of the key wait in `held`.
pub(super) struct Window {
    pub(super) machine: InitializedStateMachine<Facts>,
    pub(super) opening: Option<Opening>,
    /// The value the streams got last.
    pub(super) announced: Option<WindowState>,
}

pub(super) struct Opening {
    pub(super) read_id: u64,
    pub(super) generation: u64,
    pub(super) held: Vec<Ev>,
}

/// What a window knows of its owner when it opens.
pub(super) struct WindowBasis {
    pub(super) epoch: Epoch,
    pub(super) online: Option<bool>,
    pub(super) online_seq: u64,
    pub(super) saving: bool,
    pub(super) generation: u64,
}

impl Window {
    pub(super) fn new(model: String, basis: WindowBasis, (max_pages, members_complete, lifetime, media, local_order, reads_node): (u32, bool, (i64, i64), bool, bool, bool), read_id: u64, ids: (&mut RequestIds, &mut u32), now: i64) -> Self {
        let WindowBasis { epoch, online, online_seq, saving, generation } = basis;
        let opening = Some(Opening { read_id, generation, held: Vec::new() });
        let facts = Facts {
            dependency_models: BTreeSet::from([model]),
            epoch,
            online,
            online_seq,
            load_seq: std::cell::Cell::new(0),
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
            local_order,
            max_pages,
            members_complete,
            cursor_reset_used: false,
            replace_all: false,
            reread_due: false,
            has_rows: false,
            outcome: LastLoad::None,
            landed_revision: 0,
            store_seen: 0,
            request_base: std::cell::Cell::new(0),
            end_cause: EndCause::SessionEnded,
            stale_marked: std::cell::Cell::new(false),
            stale_owed: std::cell::Cell::new(false),
            media,
            marker: None,
            members: 0,
            landing_marker: None,
            reads_node,
            reread_by_event: std::cell::Cell::new(false),
            fx: RefCell::new(Vec::new()),
        };
        let mut ctx = Ctx { request_ids: ids.0, next_attempt: ids.1, now };
        Self { machine: facts.uninitialized_state_machine().init_with_context(&mut ctx), opening, announced: None }
    }

    pub(super) fn handle(&mut self, event: Ev, ids: (&mut RequestIds, &mut u32), now: i64) -> Vec<Emit> {
        if let Some(opening) = &mut self.opening {
            match event {
                Ev::EpochEnded { .. } | Ev::Closing => {
                    let cause = if let Ev::EpochEnded { cause } = &event { cause.clone() } else { EndCause::Closed };
                    let held = std::mem::take(&mut opening.held);
                    self.opening = None;
                    let mut effects: Vec<Emit> = held.iter().filter_map(|held| if let Ev::Load { id, .. } = held { Some(Emit::Run(WindowEffect::Outcome { id: *id, outcome: WindowOutcome::Ended { cause: cause.clone() } })) } else { None }).collect();
                    effects.extend(self.run(event, ids, now));
                    return effects;
                }
                Ev::StoreSeen { .. } | Ev::SourceChanged { .. } => return self.run(event, ids, now),
                Ev::Foreground { .. } => return Vec::new(),
                held => {
                    opening.held.push(held);
                    return Vec::new();
                }
            }
        }
        self.run(event, ids, now)
    }

    /// The saved window arrived: the machine takes it if its generation matches, then runs the held inputs in order.
    pub(super) fn opened(&mut self, saved: Option<SavedWindow>, ids: (&mut RequestIds, &mut u32), now: i64) -> Vec<Emit> {
        let Some(opening) = self.opening.take() else { return Vec::new() };
        let saved = saved.filter(|saved| saved.generation == opening.generation);
        let (request_ids, next_attempt) = ids;
        let mut effects = self.run(Ev::Saved(saved), (&mut *request_ids, &mut *next_attempt), now);
        for held in opening.held {
            effects.extend(self.run(held, (&mut *request_ids, &mut *next_attempt), now));
        }
        effects
    }

    pub(super) fn run(&mut self, event: Ev, ids: (&mut RequestIds, &mut u32), now: i64) -> Vec<Emit> {
        let previous = self.request_id();
        let mut ctx = Ctx { request_ids: ids.0, next_attempt: ids.1, now };
        self.machine.handle_with_context(&event, &mut ctx);
        let mut effects = self.machine.fx.take();
        if let Some(request) = previous.filter(|request| Some(*request) != self.request_id()) {
            effects.insert(0, Emit::Unpin { request });
        }
        let value = self.load_state();
        if self.announced.as_ref() != Some(&value) && !matches!(self.machine.state(), WinState::Ended {}) && !effects.iter().any(|effect| matches!(effect, Emit::Announce { .. })) {
            effects.push(Emit::Announce { streams: self.machine.interest.iter().copied().filter(|id| *id != PRELOAD).collect() });
        }
        if effects.iter().any(|effect| matches!(effect, Emit::Announce { .. })) { self.announced = Some(value); }
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

    pub(super) fn load_state(&self) -> WindowState { self.machine.load_state(self.opening.is_none() && matches!(self.machine.state(), WinState::Loading { .. } | WinState::Landing { .. })) }

    pub(super) fn reading(&self, read_id: u64) -> bool { self.opening.as_ref().is_some_and(|opening| opening.read_id == read_id) }

    pub(super) fn owns(&self, id: RequestId) -> bool {
        match self.machine.state() {
            WinState::Loading { load } => load.request == id || load.step == id,
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
pub type HasLocalOrder = Arc<dyn Fn(&WindowKey) -> bool + Send + Sync>;
pub type HasMedia = Arc<dyn Fn(&WindowKey) -> bool + Send + Sync>;

/// Whether the `.dbl` declares `refresh: foreground` for the relation of a key.
pub type OnForeground = Arc<dyn Fn(&WindowKey) -> bool + Send + Sync>;

/// The state of the owner: the fields the machines read (`online`, `generation`), the keys declared `preload` and the machines.
pub struct WindowsState {
    pub(super) epoch: Epoch,
    pub(super) online: Option<bool>,
    pub(super) online_seq: u64,
    pub(super) saving: bool,
    pub(super) generation: u64,
    pub(super) preload: Vec<WindowKey>,
    pub(super) operations_awaited: bool,
    pub(super) identity: bool,
    pub(super) ended: Option<EndCause>,
    pub(super) sources: BTreeSet<WindowKey>,
    pub(super) request_ids: RequestIds,
    pub(super) next_attempt: u32,
    pub(super) windows: BTreeMap<WindowKey, Window>,
    pub(super) max_pages: MaxPages,
    pub(super) members_complete: MembersComplete,
    pub(super) lifetimes: Lifetimes,
    pub(super) media: HasMedia,
    pub(super) local_order: HasLocalOrder,
    pub(super) next_read: u64,
    pub(super) on_foreground: OnForeground,
    pub(super) foreground_seen: u64,
    pub(super) peers: WindowsPeers,
    /// `LoadNoWait` calls that came before the addressed snapshot of the epoch.
    pub(super) held_nowait: Vec<WindowsInput>,
    /// The ids of the calls that began and whose `Load` is not applied yet.
    pub(super) reserved: BTreeSet<InterestId>,
    /// `Load` inputs that wait for the smaller reserved ids.
    pub(super) held_loads: BTreeMap<InterestId, WindowsInput>,
    /// `InterestMinus` of a registered call with the ids that were reserved when it came: it applies after they registered.
    pub(super) held_leaves: Vec<(BTreeSet<InterestId>, WindowKey, InterestId)>,
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
        Self { epoch, online, online_seq: 0, saving: false, generation: 0, preload: Vec::new(), operations_awaited: false, identity: false, ended: None, sources: BTreeSet::new(), request_ids: RequestIds::Process, next_attempt: 1, windows: BTreeMap::new(), max_pages: Arc::new(|_| u32::MAX), members_complete: Arc::new(|_| false), lifetimes: Arc::new(|_| (300_000, 300_000)), media: Arc::new(|_| false), local_order: Arc::new(|_| false), next_read: 1, on_foreground: Arc::new(|_| false), foreground_seen: 0, peers, held_nowait: Vec::new(), reserved: BTreeSet::new(), held_loads: BTreeMap::new(), held_leaves: Vec::new() }
    }

    #[cfg(test)]
    pub fn with_local_request_ids(mut self) -> Self {
        self.request_ids = RequestIds::Local(1);
        self
    }

    /// Keys that `EpochStarted` loads with `Interest+(PRELOAD)`.
    pub fn with_preload(mut self, keys: Vec<WindowKey>) -> Self {
        self.preload = keys;
        self
    }

    /// The preload keys wait for `RegisterOperations`: a request built before its operation is declared fails.
    pub fn with_operations_awaited(mut self) -> Self {
        self.operations_awaited = true;
        self
    }

    /// Keys whose rows name other keys to preload: a Store commit of their model asks `DerivePreload`.
    pub fn with_sources(mut self, keys: Vec<WindowKey>) -> Self {
        self.sources = keys.into_iter().collect();
        self
    }

    #[cfg(test)]
    pub fn held(&self) -> usize { self.held_nowait.len() }

    pub fn with_members_complete(mut self, members_complete: MembersComplete) -> Self {
        self.members_complete = members_complete;
        self
    }

    pub fn with_local_order(mut self, local_order: HasLocalOrder) -> Self {
        self.local_order = local_order;
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

    pub fn load_state(&self, key: &WindowKey) -> Option<WindowState> { self.windows.get(key).map(Window::load_state) }

    pub fn windows(&self) -> usize { self.windows.len() }

    /// The read of the saved window the key waits for.
    pub fn reading(&self, key: &WindowKey) -> Option<u64> { self.windows.get(key).and_then(|window| window.opening.as_ref().map(|opening| opening.read_id)) }

    /// A key without a machine gets one in `Opening` and asks for its saved window.
    pub(super) fn ensure(&mut self, key: &WindowKey, now: i64, out: &mut Effects<Windows>) {
        if !self.windows.contains_key(key) && self.ended.is_none() {
            let read_id = self.next_read;
            self.next_read += 1;
            self.create(key, read_id, now);
            out.push(Effect::Run(WindowsEffect::ReadWindow { key: key.clone(), read_id }));
        }
    }

    fn create(&mut self, key: &WindowKey, read_id: u64, now: i64) {
        let ids = (&mut self.request_ids, &mut self.next_attempt);
        let window = Window::new(key.model.clone(), WindowBasis { epoch: self.epoch, online: self.online, online_seq: self.online_seq, saving: self.saving, generation: self.generation }, ((self.max_pages)(key), (self.members_complete)(key), (self.lifetimes)(key), (self.media)(key), (self.local_order)(key), key.reads_node()), read_id, ids, now);
        self.windows.insert(key.clone(), window);
    }
}

pub struct Windows;

impl Windows {
    pub(super) fn on(state: &mut WindowsState, key: &WindowKey, event: Ev, now: i64, out: &mut Effects<Self>) {
        let Some(window) = state.windows.get_mut(key) else { return };
        let emits = window.handle(event, (&mut state.request_ids, &mut state.next_attempt), now);
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
                Emit::Commit { landed_at, request, page, number, replace_all, base_revision, marker, keep_rows, source } => {
                    let mut ops = Vec::new();
                    if !keep_rows {
                        ops.push(StoreOp::Window { landed_at, key: key.name.clone(), model: key.model.clone(), page: number, replace_all, cursor: page.cursor.clone(), roots: page.payload.roots.clone(), related: page.payload.related.clone(), total: page.payload.total, base_revision, source });
                    }
                    if let Some(marker) = marker {
                        ops.push(StoreOp::WindowMarker { landed_at: (number == 1).then_some(landed_at), key: key.name.clone(), model: key.model.clone(), marker });
                    }
                    let batch = StoreBatch { author: BatchAuthor::Window, id: request, stamp: crate::runtime::EpochStamp::Epoch(epoch), rows: Vec::new(), intents: 0, credential: None, ops };
                    out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Commit(batch))));
                }
                Emit::Retry { request, attempt, class } => {
                    let policy = retry_policy(class).unwrap_or(RetryPolicy::QueryServer);
                    out.push(Effect::Backoff { id: ScheduleId(request), policy, attempt, input: WindowsInput::RetryDue { request } });
                }
                Emit::CancelRetry { request } => out.push(Effect::Cancel(ScheduleId(request))),
                Emit::Announce { streams } => {
                    let state = window.load_state();
                    for stream in streams {
                        out.push(Effect::Send(Outbound::new(peers.deliveries, DeliveryInput::WindowState { stream, state: state.clone() })));
                    }
                }
            }
        }
    }

    /// Applies the held loads whose smaller ids are all registered, in id order, then the held leaves whose reserved calls all registered.
    fn apply_held(state: &mut WindowsState, turn: crate::runtime::Time) -> Effects<Self> {
        let now = i64::try_from(turn.0).unwrap_or(i64::MAX);
        let mut out: Effects<Self> = Vec::new();
        while let Some(id) = state.held_loads.keys().next().copied().filter(|id| state.reserved.range(..*id).next().is_none()) {
            if let Some(input) = state.held_loads.remove(&id) {
                out.extend(Self::step(state, input, turn));
            }
        }
        let reserved = state.reserved.clone();
        for (waiting, _, _) in &mut state.held_leaves {
            waiting.retain(|id| reserved.contains(id));
        }
        while let Some(position) = state.held_leaves.iter().position(|(waiting, _, _)| waiting.is_empty()) {
            let (_, key, id) = state.held_leaves.remove(position);
            Self::on(state, &key, Ev::InterestMinus { id }, now, &mut out);
        }
        out
    }

    fn interrupt_held(state: &mut WindowsState, cause: EndCause, out: &mut Effects<Self>) {
        state.reserved.clear();
        state.held_leaves.clear();
        for held in std::mem::take(&mut state.held_loads).into_values().chain(std::mem::take(&mut state.held_nowait)) {
            if let WindowsInput::LoadNoWait { key, id, .. } | WindowsInput::Load { key, id, .. } = held {
                out.push(Effect::Run(WindowsEffect::Window { key, effect: WindowEffect::Outcome { id, outcome: WindowOutcome::Ended { cause: cause.clone() } } }));
            }
        }
    }

    pub(super) fn by_request(state: &mut WindowsState, request: RequestId, event: Ev, now: i64, out: &mut Effects<Self>) {
        let Some(key) = state.windows.iter().find(|(_, window)| window.owns(request)).map(|(key, _)| key.clone()) else { return };
        Self::on(state, &key, event, now, out);
    }

    pub(super) fn every(state: &mut WindowsState, event: &Ev, now: i64, out: &mut Effects<Self>) {
        let keys: Vec<WindowKey> = state.windows.keys().cloned().collect();
        for key in keys {
            Self::on(state, &key, event.clone(), now, out);
        }
    }

    /// A key without a machine in an ended epoch gets none: its caller is answered as an `Ended` machine would.
    fn refused_after_end(state: &WindowsState, key: &WindowKey, id: InterestId, out: &mut Effects<Self>) -> bool {
        let Some(cause) = state.ended.clone().filter(|_| !state.windows.contains_key(key)) else { return false };
        out.push(Effect::Run(WindowsEffect::Window { key: key.clone(), effect: WindowEffect::Outcome { id, outcome: WindowOutcome::Ended { cause } } }));
        true
    }

    pub(super) fn interest_plus(state: &mut WindowsState, key: WindowKey, (id, now): (InterestId, i64), out: &mut Effects<Self>) {
        if Self::refused_after_end(state, &key, id, out) { return; }
        state.ensure(&key, now, out);
        Self::on(state, &key, Ev::InterestPlus { id }, now, out);
    }

    pub(super) fn interest_preload(state: &mut WindowsState, key: WindowKey, now: i64, out: &mut Effects<Self>) {
        match state.windows.get(&key) {
            None => state.ensure(&key, now, out),
            Some(window) if window.interest().contains(&PRELOAD) => return,
            Some(_) => {}
        }
        Self::on(state, &key, Ev::InterestPlus { id: PRELOAD }, now, out);
    }

    /// The declared keys get their machines once a session is live and the operations are declared. A key that has a machine is left as it is.
    pub(super) fn start_preload(state: &mut WindowsState, now: i64, out: &mut Effects<Self>) {
        if !state.identity || state.operations_awaited { return; }
        for key in state.preload.clone() {
            Self::interest_preload(state, key, now, out);
        }
    }

    /// The RowsOf keys of a source window that rests with rows: one derivation for open, `RegisterOperations` and a source write.
    pub(super) fn derive_source(state: &mut WindowsState, key: &WindowKey, now: i64, out: &mut Effects<Self>) {
        let Some(window) = state.windows.get(key).filter(|window| state.sources.contains(key) && window.stage() == WindowStage::Idle && window.machine.has_rows) else { return };
        let revision = window.machine.store_seen;
        Self::on(state, key, Ev::SourceChanged { revision }, now, out);
    }

    pub(super) fn interest_passive(state: &mut WindowsState, key: WindowKey, (id, now): (InterestId, i64), out: &mut Effects<Self>) {
        state.ensure(&key, now, out);
        Self::on(state, &key, Ev::InterestPassive { id }, now, out);
    }

    pub(super) fn reload_on_connect(state: &mut WindowsState, key: WindowKey, (id, now): (InterestId, i64), out: &mut Effects<Self>) {
        state.ensure(&key, now, out);
        Self::on(state, &key, Ev::ReloadOnConnect { id }, now, out);
    }

    pub(super) fn load(state: &mut WindowsState, key: WindowKey, (id, now): (InterestId, i64), (goal, force, store_revision): (Goal, bool, i64), out: &mut Effects<Self>) {
        if key.is_empty_nodes() {
            let effect = WindowEffect::Outcome { id, outcome: WindowOutcome::Loaded { revision: store_revision, has_more: false, fetched: false } };
            out.push(Effect::Run(WindowsEffect::Window { key, effect }));
            return;
        }
        if Self::refused_after_end(state, &key, id, out) { return; }
        state.ensure(&key, now, out);
        Self::on(state, &key, Ev::Load { id, goal, force, store_revision }, now, out);
    }
}

impl StateOwner for Windows {
    const EPOCHED: bool = true;

    type State = WindowsState;
    type Input = WindowsInput;
    type Effect = WindowsEffect;

    fn step(state: &mut WindowsState, input: WindowsInput, turn: crate::runtime::Time) -> Effects<Self> {
        let now = i64::try_from(turn.0).unwrap_or(i64::MAX);
        let mut out: Effects<Self> = Vec::new();
        let mut released = false;
        if let WindowsInput::Load { id, .. } | WindowsInput::LoadNoWait { id, .. } = &input {
            let id = *id;
            if state.reserved.range(..id).next().is_some() {
                state.held_loads.insert(id, input);
                return out;
            }
            released = state.reserved.remove(&id);
        }
        if matches!(input, WindowsInput::LoadNoWait { .. }) && state.online.is_none() {
            state.held_nowait.push(input);
            return if released { Self::apply_held(state, turn) } else { out };
        }
        match input {
            WindowsInput::Reserve { id } => { state.reserved.insert(id); }
            WindowsInput::Unreserve { id } | WindowsInput::InterestMinus { id, .. } if state.reserved.contains(&id) => {
                state.reserved.remove(&id);
                state.held_loads.remove(&id);
                released = true;
            }
            WindowsInput::Unreserve { .. } => {}
            WindowsInput::InterestMinus { key, id } if !state.reserved.is_empty() && state.windows.get(&key).is_some_and(|window| window.waiters().contains(&id)) => {
                let waiting = state.reserved.clone();
                state.held_leaves.push((waiting, key, id));
            }
            WindowsInput::InterestPlus { key, id } => Self::interest_plus(state, key, (id, now), &mut out),
            WindowsInput::InterestPassive { key, id } => Self::interest_passive(state, key, (id, now), &mut out),
            WindowsInput::ReloadOnConnect { key, id } => Self::reload_on_connect(state, key, (id, now), &mut out),
            WindowsInput::OfflineRefused { key, message } => Self::on(state, &key, Ev::OfflineRefused { message }, now, &mut out),
            WindowsInput::InterestMinus { key, id } => Self::on(state, &key, Ev::InterestMinus { id }, now, &mut out),
            WindowsInput::Load { key, id, goal, force, store_revision } => Self::load(state, key, (id, now), (goal, force, store_revision), &mut out),
            WindowsInput::LoadNoWait { key, id, verb, .. } if state.online != Some(true) => {
                let WindowKind::Relation { relation, .. } = key.kind() else { return out };
                let message = format!("{}.{relation} {verb} needs the network", key.model);
                out = Self::step(state, WindowsInput::OfflineRefused { key: key.clone(), message: message.clone() }, turn);
                out.push(Effect::Run(WindowsEffect::Window { key, effect: WindowEffect::Outcome { id, outcome: WindowOutcome::Offline { message } } }));
            }
            WindowsInput::LoadNoWait { key, id, goal, force, store_revision, .. } => Self::load(state, key, (id, now), (goal, force, store_revision), &mut out),
            WindowsInput::Pinned { request_id, base_revision } => {
                if state.windows.values().any(|window| window.owns(request_id)) {
                    Self::by_request(state, request_id, Ev::Pinned { request: request_id, base_revision }, now, &mut out);
                } else {
                    out.push(Effect::Send(Outbound::new(state.peers.store, StoreInput::Unpin { request_id })));
                }
            }
            WindowsInput::Response { request, attempt, page } => Self::by_request(state, request, Ev::Response { request, attempt, page }, now, &mut out),
            WindowsInput::Failure { request, attempt, error } => Self::by_request(state, request, Ev::Failure { request, attempt, error: error.clone() }, now, &mut out),
            WindowsInput::Dropped { request, attempt } => Self::by_request(state, request, Ev::Failure { request, attempt, error: RequestError { class: ErrorClass::Network, failure: crate::ffi::DbError::Dropped { detail: "the request left and the answer did not come".into() } } }, now, &mut out),
            WindowsInput::Committed(committed) => Self::by_request(state, committed.batch, Ev::Committed { request: committed.batch, revision: committed.revision }, now, &mut out),
            WindowsInput::Rejected(rejected) => Self::by_request(state, rejected.batch, Ev::Rejected { request: rejected.batch, reason: rejected.reason }, now, &mut out),
            WindowsInput::Stale { model, keys, cause } => {
                let named: Vec<WindowKey> = state.windows.keys().filter(|key| key.model == model && keys.names(key)).cloned().collect();
                for key in named {
                    Self::on(state, &key, Ev::Stale { cause }, now, &mut out);
                }
            }
            WindowsInput::RetryDue { request } => Self::by_request(state, request, Ev::RetryDue { request }, now, &mut out),
            WindowsInput::PreloadKeys { keys } => {
                let dropped: Vec<WindowKey> = state.windows.iter().filter(|(key, window)| !keys.contains(key) && !state.preload.contains(key) && window.interest().contains(&PRELOAD)).map(|(key, _)| key.clone()).collect();
                for key in dropped {
                    Self::on(state, &key, Ev::InterestMinus { id: PRELOAD }, now, &mut out);
                }
                for key in keys {
                    Self::interest_preload(state, key, now, &mut out);
                }
            }
            WindowsInput::RegisterOperations => {
                state.operations_awaited = false;
                Self::start_preload(state, now, &mut out);
                let sources: Vec<WindowKey> = state.sources.iter().cloned().collect();
                for key in sources {
                    Self::derive_source(state, &key, now, &mut out);
                }
            }
            WindowsInput::StoreCommitted { models, revision } => {
                let keys: Vec<WindowKey> = state.windows.keys().cloned().collect();
                for key in keys {
                    Self::on(state, &key, Ev::StoreSeen { revision }, now, &mut out);
                }
                let sources: Vec<WindowKey> = state.sources.iter().filter(|key| models.contains(&key.model) && state.windows.contains_key(*key)).cloned().collect();
                for key in sources {
                    Self::on(state, &key, Ev::SourceChanged { revision }, now, &mut out);
                }
            }
            WindowsInput::Foreground { seq, online_seq } if seq > state.foreground_seen => {
                state.foreground_seen = seq;
                state.online_seq = state.online_seq.max(online_seq);
                let named: Vec<WindowKey> = state.windows.keys().filter(|key| (state.on_foreground)(key)).cloned().collect();
                for key in named {
                    Self::on(state, &key, Ev::Foreground { online_seq }, now, &mut out);
                }
            }
            WindowsInput::Foreground { .. } => {}
            WindowsInput::Online { online: Online(online), online_seq } => {
                state.online = Some(online);
                if online {
                    state.online_seq = online_seq;
                }
                Self::every(state, &Ev::Online { online, online_seq }, now, &mut out);
                for held in std::mem::take(&mut state.held_nowait) {
                    out.extend(Self::step(state, held, turn));
                }
            }
            WindowsInput::PowerSaving { saving } => {
                state.saving = saving;
                Self::every(state, &Ev::PowerSaving { saving }, now, &mut out);
            }
            WindowsInput::WindowRead { key, read_id, result } => {
                let Some(window) = state.windows.get_mut(&key).filter(|window| window.reading(read_id)) else { return out };
                let saved = result.unwrap_or(None);
                let emits = window.opened(saved, (&mut state.request_ids, &mut state.next_attempt), now);
                Self::emitted(state, &key, emits, &mut out);
                Self::derive_source(state, &key, now, &mut out);
            }
            WindowsInput::TaskFailed { id } if id & REQUEST_TASK != 0 => {
                let (request, attempt) = task_request(id);
                let error = RequestError { class: ErrorClass::Permanent, failure: crate::ffi::DbError::Runtime { detail: "the request task failed".into() } };
                return Self::step(state, WindowsInput::Failure { request, attempt, error }, turn);
            }
            WindowsInput::TaskFailed { id } => {
                let read_id = id & !READ_TASK;
                let Some(key) = state.windows.iter().find(|(_, window)| window.reading(read_id)).map(|(key, _)| key.clone()) else { return out };
                return Self::step(state, WindowsInput::WindowRead { key, read_id, result: Err("the read task failed".into()) }, turn);
            }
            WindowsInput::EpochStarted(started) => {
                state.epoch = started.epoch;
                state.generation = started.generation;
                state.windows.clear();
                state.online = None;
                state.online_seq = 0;
                state.saving = false;
                state.foreground_seen = 0;
                state.identity = started.identity.is_some();
                state.ended = None;
                if state.identity {
                    out.push(Effect::Run(WindowsEffect::SnapshotRequest { epoch: started.epoch }));
                    Self::start_preload(state, now, &mut out);
                }
            }
            WindowsInput::EpochEnded(ended) => {
                if ended.epoch == state.epoch { state.identity = false; state.ended = Some(EndCause::of(ended.cause)); }
                let cause = EndCause::of(ended.cause);
                Self::interrupt_held(state, cause.clone(), &mut out);
                let keys: Vec<WindowKey> = state.windows.iter().filter(|(_, window)| window.machine.epoch == ended.epoch).map(|(key, _)| key.clone()).collect();
                for key in keys {
                    Self::on(state, &key, Ev::EpochEnded { cause: cause.clone() }, now, &mut out);
                }
            }
            WindowsInput::Closing => {
                Self::interrupt_held(state, EndCause::Closed, &mut out);
                Self::every(state, &Ev::Closing, now, &mut out);
                out.push(Effect::Run(WindowsEffect::Drained(Drained { owner: "windows" })));
            }
        }
        if released { out.extend(Self::apply_held(state, turn)); }
        out
    }

    fn executor_failed(_executor: &str, pending: Vec<EffectId>) -> Vec<WindowsInput> { pending.into_iter().map(|id| WindowsInput::TaskFailed { id }).collect() }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> WindowsInput { WindowsInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> WindowsInput { WindowsInput::Closing }
}
