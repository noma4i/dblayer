use super::*;

impl FeedState {
    pub(super) fn frames_sent(&mut self, now: Option<Time>, fx: &[Fx]) {
        let Some(now) = now else { return };
        for effect in fx {
            if let Fx::Run(FeedEffect::Seal { connection, .. }) = effect {
                if let Some(found) = self.connection_mut(*connection) { found.last_sent = now; }
            }
        }
    }
    pub fn new(epoch: Epoch, peers: FeedPeers, debounce: BTreeMap<String, u64>, channels: BTreeMap<String, Vec<Channel>>) -> Self {
        Self {
            epoch,
            mode: Mode::Off { gap: false },
            online: None,
            online_seq: 0,
            want: BTreeMap::new(),
            scoped: BTreeMap::new(),
            channel_variables: BTreeMap::new(),
            scope_numbers: BTreeMap::new(),
            streams: BTreeSet::new(),
            debounce,
            channels,
            subscribed: BTreeSet::new(),
            confirmed: BTreeSet::new(),
            announced: BTreeSet::new(),
            waiting: BTreeSet::new(),
            gapped: BTreeSet::new(),
            channel_attempts: BTreeMap::new(),
            slots: BTreeMap::new(),
            generation: BTreeMap::new(),
            base_generation: 0,
            position: BTreeMap::new(),
            ledger: BTreeMap::new(),
            held: Vec::new(),
            pending: Vec::new(),
            landing: Vec::new(),
            active: None,
            opening: None,
            next_connection: 1,
            next_batch: 1,
            attempts: 0,
            refused: 0,
            drained: false,
            closing_asked: false,
            peers,
        }
    }

    pub fn mode(&self) -> Mode { self.mode }

    /// The connection attempts since the last data: the first `Event` after `Ready` resets them.
    pub fn attempts(&self) -> u32 { self.attempts }

    /// The channels that the models of `want` read, counted while a connection is active.
    pub(super) fn census(&self) -> usize {
        if self.mode == Mode::Ended || self.active.is_none() {
            return 0;
        }
        self.wanted().len()
    }

    pub fn online(&self) -> Option<bool> { self.online }

    pub fn position(&self, model: &str) -> u64 { self.position.get(model).copied().unwrap_or(0) }

    pub fn held(&self) -> usize { self.held.len() }

    pub fn refused(&self) -> u64 { self.refused }

    pub fn pending(&self) -> usize { self.pending.len() }

    pub fn landing(&self) -> usize { self.landing.len() }

    pub fn wants(&self) -> Vec<String> { self.want.keys().cloned().collect() }

    pub fn ids(&self, model: &str) -> Vec<SubscriptionId> { self.want.get(model).map(|ids| ids.iter().copied().collect()).unwrap_or_default() }

    pub fn active_connection(&self) -> Option<ConnectionId> { self.active.as_ref().map(|connection| connection.id) }

    pub fn opening_connection(&self) -> Option<ConnectionId> { self.opening.as_ref().map(|connection| connection.id) }

    pub fn protocol(&self, id: ConnectionId) -> Option<Protocol> {
        [&self.active, &self.opening].into_iter().flatten().find(|connection| connection.id == id).map(|connection| connection.protocol)
    }

    pub(super) fn models(&self) -> Vec<String> { self.want.keys().cloned().collect() }

    pub(super) fn channel_slot(&mut self, channel: &Channel) -> u64 {
        let next = self.slots.len() as u64;
        *self.slots.entry(channel.clone()).or_insert(next)
    }

    pub(super) fn scoped_channel(&mut self, scoped: &ScopedChannel) -> Channel {
        let next = self.scope_numbers.len() as u64 + 1;
        let number = *self.scope_numbers.entry((scoped.subscription.clone(), serde_json::Value::Object(scoped.variables.clone()).to_string())).or_insert(next);
        let channel = format!("{}-{number}", scoped.subscription);
        self.channel_variables.insert(channel.clone(), scoped.variables.clone());
        channel
    }

    pub(super) fn subscribe_frame(&self, channel: Channel) -> Outgoing {
        let variables = self.channel_variables.get(&channel).cloned();
        Outgoing::Subscribe { channel, variables }
    }

    pub(super) fn channels_of(&self, model: &str) -> BTreeSet<Channel> {
        let own = self.channels.get(model).into_iter().flatten();
        let windows = self.want.get(model).into_iter().flatten().filter_map(|id| self.scoped.get(id)).flatten();
        own.chain(windows).cloned().collect()
    }

    /// The channels the models of `want` read.
    pub fn wanted(&self) -> BTreeSet<Channel> { self.want.keys().flat_map(|model| self.channels_of(model)).collect() }

    pub fn confirmed(&self) -> Vec<Channel> { self.confirmed.iter().cloned().collect() }

    pub fn subscribed(&self) -> Vec<Channel> { self.subscribed.iter().cloned().collect() }

    pub(super) fn model_is_ready(&self, model: &str) -> bool { self.channels_of(model).iter().all(|channel| self.confirmed.contains(channel)) }

    pub(super) fn models_of(&self, channel: &str) -> Vec<String> { self.want.keys().filter(|model| self.channels_of(model).contains(channel)).cloned().collect() }

    pub(super) fn debounce_of(&self, model: &str) -> u64 { self.debounce.get(model).copied().unwrap_or(0) }

    pub(super) fn generation_of(&self, model: &str) -> u64 { self.generation.get(model).copied().unwrap_or(self.base_generation) }

    pub(super) fn is_online(&self) -> bool { self.online == Some(true) }

    pub(super) fn fresh_connection(&mut self) -> Connection {
        let id = self.next_connection;
        self.next_connection += 1;
        Connection { id, protocol: Protocol::Handshake, pong: None, last_sent: Time(0), pong_due: None }
    }

    /// Transport `Subscribe(want)` on a new connection.
    pub(super) fn open_connection(&mut self, fx: &mut Vec<Fx>) {
        let connection = self.fresh_connection();
        fx.push(Fx::Run(FeedEffect::Connect { connection: connection.id }));
        fx.push(Fx::HandshakeDeadline { connection: connection.id });
        self.opening = Some(connection);
    }

    /// A new `opening_connection` that waits for `Resubscribe` by the backoff.
    pub(super) fn wait_resubscribe(&mut self, fx: &mut Vec<Fx>) {
        let connection = self.fresh_connection();
        self.attempts += 1;
        fx.push(Fx::Resubscribe { connection: connection.id, attempt: self.attempts });
        self.opening = Some(connection);
    }

    pub(super) fn connection_mut(&mut self, id: ConnectionId) -> Option<&mut Connection> {
        [&mut self.active, &mut self.opening].into_iter().flatten().find(|connection| connection.id == id)
    }

    pub(super) fn seal(connection: &mut Connection, frame: Outgoing, fx: &mut Vec<Fx>) {
        if let Protocol::Established { key_id, send_counter, .. } = &mut connection.protocol {
            fx.push(Fx::Run(FeedEffect::Seal { connection: connection.id, key_id: *key_id, counter: *send_counter, frame }));
            *send_counter += 1;
            fx.push(Fx::Keepalive { connection: connection.id, counter: *send_counter });
        }
    }

    /// Transport `Unsubscribe(connection)`: an established connection says goodbye, then closes.
    pub(super) fn close(connection: Option<Connection>, subscribed: &BTreeSet<Channel>, drain: bool, fx: &mut Vec<Fx>) {
        let Some(mut connection) = connection else { return };
        for channel in subscribed {
            Self::seal(&mut connection, Outgoing::Unsubscribe { channel: channel.clone() }, fx);
        }
        fx.push(Fx::CancelResubscribe { connection: connection.id });
        fx.push(Fx::CancelHandshake { connection: connection.id });
        fx.push(Fx::CancelKeepalive { connection: connection.id });
        fx.push(Fx::CancelPong { connection: connection.id });
        let drain = drain && connection.goodbye();
        fx.push(Fx::Run(FeedEffect::Close { connection: connection.id, goodbye: connection.goodbye(), drain, intended: true }));
        if drain {
            fx.push(Fx::Drain { connection: connection.id });
        }
    }

    /// Transport `Unsubscribe` of both connections, the active one first.
    pub(super) fn close_all(&mut self, drain: bool, fx: &mut Vec<Fx>) {
        Self::close(self.active.take(), &self.subscribed, drain, fx);
        Self::close(self.opening.take(), &BTreeSet::new(), false, fx);
    }

    pub(super) fn accept(&mut self, event: FramedEvent) -> FeedEvent {
        let position = self.position(&event.model) + 1;
        self.position.insert(event.model.clone(), position);
        self.ledger.entry(event.model.clone()).or_insert_with(Ledger::new).slots.insert(position, Slot::Pending);
        let basis = self.generation_of(&event.model);
        FeedEvent { model: event.model, payload: event.payload, reread: event.reread, basis, position, refused: event.refused }
    }

    /// The positions of `count` lost events of `model`.
    pub(super) fn lose(&mut self, model: &str, count: u64, fx: &mut Vec<Fx>) {
        let at = self.position(model);
        self.position.insert(model.to_string(), at + count);
        let ledger = self.ledger.entry(model.to_string()).or_insert_with(Ledger::new);
        for position in at + 1..=at + count {
            ledger.slots.insert(position, Slot::Lost);
        }
        ledger.emit(model, fx);
    }

    pub(super) fn mark_lost(&mut self, events: &[FeedEvent], fx: &mut Vec<Fx>) -> Vec<String> {
        let mut models: Vec<String> = Vec::new();
        for event in events {
            if let Some(ledger) = self.ledger.get_mut(&event.model) {
                ledger.slots.insert(event.position, Slot::Lost);
            }
            if !models.contains(&event.model) {
                models.push(event.model.clone());
            }
        }
        for model in &models {
            if let Some(ledger) = self.ledger.get_mut(model) {
                ledger.emit(model, fx);
            }
        }
        models
    }

    pub(super) fn stale_all(models: &[String], fx: &mut Vec<Fx>) {
        for model in models {
            fx.push(Fx::Stale { model: model.clone(), keys: StaleKeys::All, cause: StaleCause::Change });
        }
    }

    pub(super) fn stale_gap(&self, models: &[String], fx: &mut Vec<Fx>) {
        for model in models {
            fx.push(Fx::Stale { model: model.clone(), keys: StaleKeys::All, cause: StaleCause::Gap { online_seq: self.online_seq } });
        }
    }

    /// `held` -> `Lost` (all, or only the models in `only`): the models whose windows turn `Stale`.
    pub(super) fn lose_held(&mut self, only: Option<&[String]>, fx: &mut Vec<Fx>) -> Vec<String> {
        let (lost, kept): (Vec<FeedEvent>, Vec<FeedEvent>) = std::mem::take(&mut self.held).into_iter().partition(|event| only.is_none_or(|only| only.contains(&event.model)));
        self.held = kept;
        self.mark_lost(&lost, fx)
    }

    pub(super) fn commit(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<Fx>) {
        let batch = self.next_batch;
        self.next_batch += 1;
        self.landing.push((batch, events.clone()));
        fx.push(Fx::Commit { batch, events });
    }

    /// One batch per equal basis, in the order the bases first appear.
    pub(super) fn commit_by_basis(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<Fx>) {
        let mut groups: Vec<(u64, Vec<FeedEvent>)> = Vec::new();
        for event in events {
            match groups.iter_mut().find(|(basis, _)| *basis == event.basis) {
                Some((_, group)) => group.push(event),
                None => groups.push((event.basis, vec![event])),
            }
        }
        for (_, group) in groups {
            self.commit(group, fx);
        }
    }

    pub(super) fn flush_pending(&mut self, fx: &mut Vec<Fx>) {
        if self.pending.is_empty() {
            return;
        }
        let events = std::mem::take(&mut self.pending);
        fx.push(Fx::CancelDebounce);
        self.commit_by_basis(events, fx);
    }

    pub(super) fn observed(&mut self, batch: BatchId, payloads: Vec<String>) {
        let Some((_, events)) = self.landing.iter_mut().find(|(id, _)| *id == batch) else { return };
        if payloads.len() == events.len() {
            for (event, payload) in events.iter_mut().zip(payloads) {
                event.payload = payload;
            }
        }
    }

    pub(super) fn owns_landing(&self, batch: BatchId) -> bool { self.landing.iter().any(|(id, _)| *id == batch) }

    /// `Committed` and `Rejected` of a batch of `landing`: delivered on `Committed`, lost on `Rejected`. `NoSession` and `Closed` end a `Live` feed.
    pub(super) fn verdict(&mut self, batch: BatchId, rejected: Option<RejectReason>, fx: &mut Vec<Fx>) {
        let Some(at) = self.landing.iter().position(|(id, _)| *id == batch) else { return };
        let events = self.landing.remove(at).1;
        match rejected {
            None => self.land(events, fx),
            Some(RejectReason::StaleGeneration { models, .. }) => {
                let (lost, rest): (Vec<FeedEvent>, Vec<FeedEvent>) = events.into_iter().partition(|event| models.contains(&event.model));
                let lost_models = self.mark_lost(&lost, fx);
                Self::stale_all(&lost_models, fx);
                if !rest.is_empty() {
                    let rest = rest.into_iter().map(|event| FeedEvent { basis: self.generation_of(&event.model), ..event }).collect();
                    self.commit_by_basis(rest, fx);
                }
            }
            Some(RejectReason::Storage(_) | RejectReason::Invalid) => {
                let lost_models = self.mark_lost(&events, fx);
                Self::stale_all(&lost_models, fx);
            }
            Some(RejectReason::NoSession | RejectReason::Closed) => {
                self.end(false, fx);
            }
        }
    }

    pub(super) fn land(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<Fx>) {
        let mut models: Vec<String> = Vec::new();
        for event in &events {
            if let Some(ledger) = self.ledger.get_mut(&event.model) {
                ledger.slots.insert(event.position, Slot::Landed(event.payload.clone()));
            }
            if !models.contains(&event.model) {
                models.push(event.model.clone());
            }
        }
        for model in &models {
            if let Some(ledger) = self.ledger.get_mut(model) {
                ledger.emit(model, fx);
            }
        }
        for model in models {
            let keys = events.iter().filter(|event| event.model == model).fold(StaleKeys::none(), |keys, event| keys.union(event.reread.clone()));
            if !keys.is_empty() {
                fx.push(Fx::Stale { model, keys, cause: StaleCause::Change });
            }
        }
    }

    /// The active connection starts: every wanted channel is subscribed, a model without a channel to wait for is ready at once.
    pub(super) fn begin_connection(&mut self, gap: bool, fx: &mut Vec<Fx>) {
        self.confirmed.clear();
        self.announced.clear();
        self.waiting.clear();
        self.subscribed.clear();
        self.channel_attempts.clear();
        self.gapped = if gap { self.want.keys().cloned().collect() } else { BTreeSet::new() };
        self.subscribe_missing(fx);
        self.announce_ready(fx);
    }

    /// `Subscribe` of every wanted channel the active connection has not subscribed yet.
    pub(super) fn subscribe_missing(&mut self, fx: &mut Vec<Fx>) {
        let wanted = self.wanted();
        if self.active.is_none() {
            return;
        }
        let fresh: Vec<Channel> = wanted.into_iter().filter(|channel| self.subscribed.insert(channel.clone())).collect();
        let frames: Vec<Outgoing> = fresh.into_iter().map(|channel| self.subscribe_frame(channel)).collect();
        if let Some(connection) = self.active.as_mut() {
            for frame in frames {
                Self::seal(connection, frame, fx);
            }
        }
    }

    /// `Unsubscribe` of the channels no model of `want` reads.
    pub(super) fn narrow(&mut self, fx: &mut Vec<Fx>) {
        let wanted = self.wanted();
        let gone: Vec<Channel> = self.subscribed.iter().filter(|channel| !wanted.contains(*channel)).cloned().collect();
        for channel in gone {
            self.subscribed.remove(&channel);
            self.confirmed.remove(&channel);
            self.channel_attempts.remove(&channel);
            if let Some(connection) = self.active.as_mut() {
                Self::seal(connection, Outgoing::Unsubscribe { channel }, fx);
            }
        }
    }

    /// `ModelGap` and `Stale` of the models that owe a gap, then `ModelReady` of every model whose channels are all confirmed; the frames `held` for them sit.
    pub(super) fn announce_ready(&mut self, fx: &mut Vec<Fx>) {
        let ready: Vec<String> = self.want.keys().filter(|model| !self.announced.contains(*model) && self.model_is_ready(model)).cloned().collect();
        if ready.is_empty() {
            return;
        }
        let gapped: Vec<String> = ready.iter().filter(|model| self.gapped.remove(*model)).cloned().collect();
        for model in &gapped {
            fx.push(Fx::ModelGap { model: model.clone() });
        }
        self.stale_gap(&gapped, fx);
        for model in &ready {
            fx.push(Fx::ModelReady { model: model.clone(), id: None });
            self.announced.insert(model.clone());
        }
        let (mine, other): (Vec<FeedEvent>, Vec<FeedEvent>) = std::mem::take(&mut self.held).into_iter().partition(|event| ready.contains(&event.model));
        self.held = other;
        self.seat(mine, fx);
    }

    /// The models announced before whose channels are all confirmed again: the readers that joined during the recovery get their `ModelReady` once, then the frames `held` for them sit.
    pub(super) fn release_recovered(&mut self, fx: &mut Vec<Fx>) {
        let readers: Vec<(String, SubscriptionId)> = self.want.iter().filter(|(model, _)| self.announced.contains(*model) && self.model_is_ready(model)).flat_map(|(model, ids)| ids.iter().filter(|id| self.waiting.contains(id)).map(|id| (model.clone(), *id))).collect();
        for (model, id) in readers {
            self.waiting.remove(&id);
            fx.push(Fx::ModelReady { model, id: Some(id) });
        }
        let (mine, other): (Vec<FeedEvent>, Vec<FeedEvent>) = std::mem::take(&mut self.held).into_iter().partition(|event| self.announced.contains(&event.model) && self.model_is_ready(&event.model));
        self.held = other;
        self.seat(mine, fx);
    }

    /// Every model of `want` has a break of unknown length: `ModelGap`, `Stale` of its windows.
    pub(super) fn gap_all(&self, fx: &mut Vec<Fx>) {
        for model in self.models() {
            fx.push(Fx::ModelGap { model });
        }
        Self::stale_all(&self.models(), fx);
    }

    /// `Event` of the current connection in `Live`: the event waits for the debounce of its model or lands at once.
    pub(super) fn seat(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<Fx>) {
        let (refused, events): (Vec<FeedEvent>, Vec<FeedEvent>) = events.into_iter().partition(|event| event.refused);
        self.refused += refused.len() as u64;
        self.mark_lost(&refused, fx);
        let mut now: Vec<FeedEvent> = Vec::new();
        let mut scheduled: Option<u64> = None;
        for event in events {
            let debounce = self.debounce_of(&event.model);
            if debounce == 0 {
                now.push(event);
                continue;
            }
            if self.pending.is_empty() && scheduled.is_none() {
                scheduled = Some(debounce);
            }
            self.pending.push(event);
        }
        if let Some(after_ms) = scheduled {
            fx.push(Fx::Debounce { after_ms });
        }
        self.commit_by_basis(now, fx);
    }

    pub(super) fn ready(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        match self.mode {
            Mode::Opening { gap } if self.opening_connection() == Some(connection) => {
                self.active = self.opening.take();
                self.mode = Mode::Live;
                self.begin_connection(gap, fx);
            }
            _ => {}
        }
    }

}
