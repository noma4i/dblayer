use super::*;

impl FeedState {
    pub(super) fn event(&mut self, connection: ConnectionId, channel: &str, event: FramedEvent, fx: &mut Vec<Fx>) {
        let current = match self.mode {
            Mode::Opening { .. } => self.opening_connection() == Some(connection),
            Mode::Live => self.active_connection() == Some(connection) && self.subscribed.contains(channel),
            _ => false,
        };
        if !current { return; }
        if !self.want.contains_key(&event.model) {
            self.refused += u64::from(event.refused);
            return;
        }
        match self.mode {
            Mode::Opening { .. } if self.opening_connection() == Some(connection) => {
                let accepted = self.accept(event);
                self.held.push(accepted);
            }
            Mode::Live if self.active_connection() == Some(connection) && self.subscribed.contains(channel) => {
                let ready = self.model_is_ready(&event.model);
                self.channel_attempts.remove(channel);
                self.attempts = 0;
                let accepted = self.accept(event);
                if ready {
                    self.seat(vec![accepted], fx);
                } else {
                    self.held.push(accepted);
                }
            }
            _ => {}
        }
    }

    pub(super) fn channel_confirmed(&mut self, connection: ConnectionId, channel: Channel, fx: &mut Vec<Fx>) {
        if self.mode != Mode::Live || self.active_connection() != Some(connection) || !self.subscribed.contains(&channel) || !self.confirmed.insert(channel.clone()) {
            return;
        }
        let ready: Vec<String> = self.models_of(&channel).into_iter().filter(|model| self.announced.contains(model) && self.model_is_ready(model)).collect();
        let regapped: Vec<(String, u64)> = ready.into_iter().filter_map(|model| self.gapped.remove(&model).map(|seq| (model, seq))).collect();
        for (model, _) in &regapped {
            fx.push(Fx::ModelGap { model: model.clone() });
        }
        self.stale_gap(&regapped, fx);
        self.announce_ready(fx);
        self.release_recovered(fx);
    }

    /// The server closed one channel: without an error the channel subscribes again by the backoff, with an error its models leave `want`.
    pub(super) fn channel_ended(&mut self, connection: ConnectionId, channel: Channel, error: Option<ChannelError>, fx: &mut Vec<Fx>) {
        if self.mode != Mode::Live || self.active_connection() != Some(connection) || !self.subscribed.contains(&channel) {
            return;
        }
        let models = self.models_of(&channel);
        self.confirmed.remove(&channel);
        if error.is_none_or(|error| error.retryable) {
            fx.push(Fx::Gap(models.clone()));
            for model in models.into_iter().filter(|model| self.announced.contains(model)) {
                self.gapped.insert(model, self.online_seq);
            }
            let slot = self.channel_slot(&channel);
            let attempt = self.channel_attempts.entry(channel.clone()).or_insert(0);
            *attempt += 1;
            fx.push(Fx::ResubscribeChannel { connection, channel, slot, attempt: *attempt });
            return;
        }
        let mut ids: Vec<SubscriptionId> = Vec::new();
        for model in models {
            self.announced.remove(&model);
            self.gapped.remove(&model);
            let removed = self.want.remove(&model).unwrap_or_default();
            removed.iter().for_each(|id| { self.scoped.remove(id); self.waiting.remove(id); });
            ids.extend(removed.into_iter().filter(|id| self.streams.remove(id)));
        }
        self.subscribed.remove(&channel);
        self.channel_attempts.remove(&channel);
        if !ids.is_empty() {
            fx.push(Fx::ModelInvalid { ids });
        }
        self.narrow(fx);
        if self.want.is_empty() {
            self.flush_pending(fx);
            self.drain_live(false, fx);
        }
    }

    pub(super) fn channel_resubscribe(&mut self, connection: ConnectionId, channel: Channel, fx: &mut Vec<Fx>) {
        if self.mode != Mode::Live || self.active_connection() != Some(connection) || !self.subscribed.contains(&channel) || self.confirmed.contains(&channel) {
            return;
        }
        let frame = self.subscribe_frame(channel);
        if let Some(found) = self.active.as_mut() {
            Self::seal(found, frame, fx);
        }
    }

    /// `Dropped` and `SubscriptionEnded` of a known connection.
    pub(super) fn dropped(&mut self, connection: ConnectionId, floor: Option<Duration>, fx: &mut Vec<Fx>) {
        if self.connection_mut(connection).is_some() {
            fx.push(Fx::CancelKeepalive { connection });
            fx.push(Fx::CancelPong { connection });
        }
        match self.mode {
            Mode::Opening { .. } if self.opening_connection() == Some(connection) => {
                let lost = self.lose_held(fx);
                self.wait_resubscribe(floor, fx);
                self.stale_lost(&lost, fx);
            }
            Mode::Live if self.active_connection() == Some(connection) => {
                self.flush_pending(fx);
                fx.push(Fx::Gap(self.want.keys().cloned().collect()));
                let lost = self.lose_held(fx);
                self.active = None;
                self.gap_seq = Some(self.online_seq);
                self.mode = Mode::Opening { gap: true };
                if self.attempts > 0 || floor.is_some() {
                    self.wait_resubscribe(floor, fx);
                } else {
                    self.attempts = 1;
                    self.open_connection(fx);
                }
                self.stale_lost(&lost, fx);
            }
            _ => {}
        }
    }

    /// `ProtocolError` of the executor: an `Established` live connection drains and reopens, any other connection is dropped (the executor closed it).
    pub(super) fn protocol_error(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        let established = self.connection_mut(connection).is_some_and(|found| found.goodbye());
        if established && self.mode == Mode::Live && self.active_connection() == Some(connection) { self.protocol_failed(connection, fx); } else { self.dropped(connection, None, fx); }
    }

    pub(super) fn protocol_failed(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        let goodbye = self.connection_mut(connection).is_some_and(|found| found.goodbye());
        if goodbye && self.mode == Mode::Live && self.active_connection() == Some(connection) {
            fx.push(Fx::Run(FeedEffect::Close { connection, goodbye, drain: true, intended: false }));
            fx.push(Fx::Drain { connection });
            self.flush_pending(fx);
            fx.push(Fx::Gap(self.want.keys().cloned().collect()));
            let lost = self.lose_held(fx);
            self.active = None;
            self.gap_seq = Some(self.online_seq);
            self.mode = Mode::Draining { connection, then: Resume::Reopen };
            self.stale_lost(&lost, fx);
            return;
        }
        fx.push(Fx::Run(FeedEffect::Close { connection, goodbye, drain: false, intended: false }));
        self.dropped(connection, None, fx);
    }

    pub(super) fn handshake_done(&mut self, connection: ConnectionId, key_id: u64, fx: &mut Vec<Fx>) {
        let Some(found) = self.connection_mut(connection) else { return };
        if found.protocol != Protocol::Handshake {
            return;
        }
        found.protocol = Protocol::Established { key_id, send_counter: FIRST_SEND_COUNTER, recv_counter: FIRST_RECV_COUNTER };
        fx.push(Fx::CancelHandshake { connection });
        self.ready(connection, fx);
    }

    pub(super) fn handshake_deadline(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        if self.protocol(connection) == Some(Protocol::Handshake) {
            self.protocol_failed(connection, fx);
        }
    }

    pub(super) fn keepalive(&mut self, connection: ConnectionId, counter: u64, now: Time, fx: &mut Vec<Fx>) {
        if self.mode != Mode::Live || self.active_connection() != Some(connection) { return; }
        let Some(found) = self.active.as_mut() else { return };
        if found.pong.is_some() || found.last_sent.is_none_or(|sent| now.0.saturating_sub(sent.0) < KEEPALIVE.as_millis() as u64) || !matches!(found.protocol, Protocol::Established { send_counter, .. } if send_counter == counter) { return; }
        Self::seal(found, Outgoing::Ping { ts: now.0 }, fx);
        found.pong = Some(counter);
        found.pong_due = Some(Time(now.0.saturating_add(PONG_DEADLINE.as_millis() as u64)));
        fx.push(Fx::PongDeadline { connection, counter });
    }

    pub(super) fn pong_deadline(&mut self, connection: ConnectionId, counter: u64, now: Time, fx: &mut Vec<Fx>) {
        if self.mode != Mode::Live || self.active_connection() != Some(connection) || !self.active.as_ref().is_some_and(|found| found.pong == Some(counter) && found.pong_due.is_some_and(|due| now >= due)) { return; }
        fx.push(Fx::Run(FeedEffect::Cancel { connection }));
        self.dropped(connection, None, fx);
    }

    pub(super) fn frame_opened(&mut self, connection: ConnectionId, counter: u64, frame: Frame, fx: &mut Vec<Fx>) {
        let Some(found) = self.connection_mut(connection) else { return };
        let accepted = match &mut found.protocol {
            Protocol::Established { recv_counter, .. } if *recv_counter == counter => {
                *recv_counter += 1;
                true
            }
            _ => false,
        };
        if !accepted {
            self.protocol_failed(connection, fx);
            return;
        }
        match frame {
            Frame::Pong => {
                if let Some(found) = self.connection_mut(connection) {
                    found.pong = None;
                    found.pong_due = None;
                    fx.push(Fx::CancelPong { connection });
                }
            }
            Frame::Ping { ts } => {
                if let Some(found) = self.connection_mut(connection) {
                    Self::seal(found, Outgoing::Pong { ts }, fx);
                }
            }
            Frame::Events { id, events } => {
                for event in events {
                    self.event(connection, &id, event, fx);
                }
            }
            Frame::Confirmed { id } => self.channel_confirmed(connection, id, fx),
            Frame::Complete { id } => self.channel_ended(connection, id, None, fx),
            Frame::Error { id: Some(id), retryable } => self.channel_ended(connection, id, Some(ChannelError { retryable }), fx),
            Frame::Ended | Frame::Error { id: None, .. } => self.dropped(connection, None, fx),
            Frame::Quiet => {}
        }
    }

    pub(super) fn resubscribe(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        let waiting = matches!(self.mode, Mode::Opening { .. }) && self.is_online() && self.opening_connection() == Some(connection);
        if !waiting {
            return;
        }
        if let Some(found) = self.opening.as_mut() {
            *found = Connection::handshake(connection);
        }
        fx.push(Fx::Run(FeedEffect::Connect { connection }));
        fx.push(Fx::HandshakeDeadline { connection });
    }

    pub(super) fn want_plus(&mut self, model: String, id: SubscriptionId, stream: bool, scoped: Vec<ScopedChannel>, fx: &mut Vec<Fx>) {
        let new = !self.want.contains_key(&model);
        let position = self.position(&model);
        self.want.entry(model.clone()).or_default().insert(id);
        if !scoped.is_empty() {
            let channels = scoped.iter().map(|scoped| self.scoped_channel(scoped)).collect();
            self.scoped.insert(id, channels);
        }
        if stream {
            self.streams.insert(id);
            fx.push(Fx::Joined { id, position });
        }
        match self.mode {
            Mode::Off { gap } if self.can_open() => {
                self.mode = Mode::Opening { gap };
                self.attempts = 0;
                self.open_connection(fx);
            }
            Mode::Live => {
                self.subscribe_missing(fx);
                if new {
                    self.announce_ready(fx);
                } else if stream && self.announced.contains(&model) {
                    if self.model_is_ready(&model) {
                        fx.push(Fx::ModelReady { model, id: Some(id) });
                    } else {
                        self.waiting.insert(id);
                    }
                }
            }
            _ => {}
        }
    }

    pub(super) fn want_minus(&mut self, model: &str, id: SubscriptionId, fx: &mut Vec<Fx>) {
        let Some(ids) = self.want.get_mut(model) else { return };
        if !ids.remove(&id) {
            return;
        }
        self.streams.remove(&id);
        self.waiting.remove(&id);
        let scoped = self.scoped.remove(&id).is_some();
        if !ids.is_empty() {
            if scoped && self.mode == Mode::Live {
                self.narrow(fx);
            }
            return;
        }
        self.want.remove(model);
        self.announced.remove(model);
        self.gapped.remove(model);
        if !self.want.is_empty() {
            if self.mode == Mode::Live {
                self.narrow(fx);
            }
            return;
        }
        match self.mode {
            Mode::Opening { gap } => {
                self.mode = Mode::Off { gap };
                self.leave(fx);
            }
            Mode::Live => {
                self.flush_pending(fx);
                self.drain_live(false, fx);
            }
            _ => {}
        }
    }

    /// `Opening` -> `Off`: `held` is lost, the connection closes.
    pub(super) fn leave(&mut self, fx: &mut Vec<Fx>) {
        self.close_all(false, fx);
        let lost = self.lose_held(fx);
        self.stale_lost(&lost, fx);
    }

    pub(super) fn online_changed(&mut self, online: bool, online_seq: u64, fx: &mut Vec<Fx>) {
        self.online = Some(online);
        if online {
            self.online_seq = online_seq;
        }
        match self.mode {
            Mode::Off { gap } if online && self.gate && !self.want.is_empty() => {
                self.mode = Mode::Opening { gap };
                self.attempts = 0;
                self.open_connection(fx);
            }
            Mode::Opening { gap } if !online => {
                self.mode = Mode::Off { gap };
                self.leave(fx);
            }
            Mode::Live if !online => {
                self.flush_pending(fx);
                self.drain_live(true, fx);
            }
            _ => {}
        }
    }

    /// `Gate` of the Store: false closes the connection like the last `Want-` with `want` kept, true opens it without Backoff.
    pub(super) fn gate_changed(&mut self, open: bool, fx: &mut Vec<Fx>) {
        if std::mem::replace(&mut self.gate, open) == open {
            return;
        }
        match self.mode {
            Mode::Off { gap } if open && self.is_online() && !self.want.is_empty() => {
                self.mode = Mode::Opening { gap };
                self.attempts = 0;
                self.open_connection(fx);
            }
            Mode::Opening { gap } if !open => {
                self.mode = Mode::Off { gap };
                self.leave(fx);
            }
            Mode::Live if !open => {
                self.flush_pending(fx);
                self.drain_live(true, fx);
            }
            _ => {}
        }
    }

    pub(super) fn end(&mut self, drained: bool, fx: &mut Vec<Fx>) {
        let draining = match self.active.as_ref() {
            Some(active) if drained && self.mode == Mode::Live && active.goodbye() => Some(active.id),
            _ => None,
        };
        self.refused = 0;
        self.close_all(draining.is_some(), fx);
        self.mode = draining.map_or(Mode::Ended, |connection| Mode::Draining { connection, then: Resume::End });
        self.closing_asked |= draining.is_some();
        self.held.clear();
        if !self.pending.is_empty() {
            self.pending.clear();
            fx.push(Fx::CancelDebounce);
        }
        if drained && draining.is_none() && !std::mem::replace(&mut self.drained, true) {
            fx.push(Fx::Run(FeedEffect::Drained(Drained { owner: "feed" })));
        }
    }

    /// A live feed leaves `Live` for `Off{gap}`: the established connection drains first.
    pub(super) fn drain_live(&mut self, gap: bool, fx: &mut Vec<Fx>) {
        if gap {
            fx.push(Fx::Gap(self.want.keys().cloned().collect()));
            self.gap_seq = Some(self.online_seq);
        }
        let draining = self.active.as_ref().filter(|active| active.goodbye()).map(|active| active.id);
        self.close_all(draining.is_some(), fx);
        self.mode = draining.map_or(Mode::Off { gap }, |connection| Mode::Draining { connection, then: Resume::Off { gap } });
    }

    /// `Draining`: the timer, or the end of the connection, cancels it once. The wants and the network state go on while the feed resumes; the epoch end cancels at once.
    pub(super) fn draining(&mut self, connection: ConnectionId, then: Resume, input: FeedInput, fx: &mut Vec<Fx>) {
        match input {
            FeedInput::Drain { connection_id } if connection_id == connection => self.drain_over(connection, then, false, fx),
            FeedInput::Dropped { connection_id } | FeedInput::Limited { connection_id, .. } | FeedInput::SubscriptionEnded { connection_id } if connection_id == connection => self.drain_over(connection, then, true, fx),
            FeedInput::EpochEnded(ended) if ended.epoch == self.epoch => self.drain_over(connection, Resume::End, true, fx),
            FeedInput::Closing => {
                self.closing_asked = true;
                self.mode = Mode::Draining { connection, then: Resume::End };
            }
            FeedInput::WantPlus { model, id, stream, scoped, .. } if then != Resume::End => self.want_plus(model, id, stream, scoped, fx),
            FeedInput::WantMinus { model, id, .. } if then != Resume::End => self.want_minus(&model, id, fx),
            FeedInput::Online { online: Online(online), online_seq, .. } if then != Resume::End => self.online_changed(online, online_seq, fx),
            FeedInput::Gate(open) if then != Resume::End => self.gate_changed(open, fx),
            _ => {}
        }
    }

    /// A terminal verdict of the Store ended the feed while a connection drained: the connection is cancelled at once.
    pub(super) fn drain_cut(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        fx.push(Fx::Run(FeedEffect::Cancel { connection }));
        fx.push(Fx::CancelDrain { connection });
        if std::mem::take(&mut self.closing_asked) {
            self.drained = true;
            fx.push(Fx::Run(FeedEffect::Drained(Drained { owner: "feed" })));
        }
    }

    fn drain_over(&mut self, connection: ConnectionId, then: Resume, timer: bool, fx: &mut Vec<Fx>) {
        fx.push(Fx::Run(FeedEffect::Cancel { connection }));
        if timer {
            fx.push(Fx::CancelDrain { connection });
        }
        match then {
            Resume::End => {
                self.mode = Mode::Ended;
                if std::mem::take(&mut self.closing_asked) {
                    self.drained = true;
                    fx.push(Fx::Run(FeedEffect::Drained(Drained { owner: "feed" })));
                }
            }
            Resume::Off { gap } => self.go_on(gap, fx),
            Resume::Reopen => self.go_on(true, fx),
        }
    }

    fn go_on(&mut self, gap: bool, fx: &mut Vec<Fx>) {
        if self.want.is_empty() || !self.can_open() {
            self.mode = Mode::Off { gap };
            return;
        }
        self.mode = Mode::Opening { gap };
        self.attempts = 0;
        self.open_connection(fx);
    }
}
