use super::*;

impl FeedState {
    pub(super) fn event(&mut self, connection: ConnectionId, channel: &str, event: FramedEvent, fx: &mut Vec<Fx>) {
        let current = match self.mode {
            Mode::Opening { .. } => self.opening_connection() == Some(connection),
            Mode::Live => self.active_connection() == Some(connection) && self.subscribed.contains(channel),
            _ => false,
        };
        if !current { return; }
        if event.refused { self.refused += 1; }
        if !self.want.contains_key(&event.model) {
            return;
        }
        match self.mode {
            Mode::Opening { .. } if self.opening_connection() == Some(connection) => {
                let accepted = self.accept(event);
                self.held.push(accepted);
            }
            Mode::Live if self.active_connection() == Some(connection) && self.subscribed.contains(channel) => {
                let ready = self.announced.contains(&event.model);
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
        let regapped: Vec<String> = self.models_of(&channel).into_iter().filter(|model| self.announced.contains(model) && self.gapped.remove(model)).collect();
        for model in &regapped {
            fx.push(Fx::ModelGap { model: model.clone() });
        }
        self.stale_gap(&regapped, fx);
        self.announce_ready(fx);
    }

    /// The server closed one channel: without an error the channel subscribes again by the backoff, with an error its models leave `want`.
    pub(super) fn channel_ended(&mut self, connection: ConnectionId, channel: Channel, failed: bool, fx: &mut Vec<Fx>) {
        if self.mode != Mode::Live || self.active_connection() != Some(connection) || !self.subscribed.contains(&channel) {
            return;
        }
        let models = self.models_of(&channel);
        self.confirmed.remove(&channel);
        if !failed {
            for model in models.into_iter().filter(|model| self.announced.contains(model)) {
                self.gapped.insert(model);
            }
            let attempt = self.channel_attempts.entry(channel.clone()).or_insert(0);
            *attempt += 1;
            fx.push(Fx::ResubscribeChannel { connection, channel, attempt: *attempt });
            return;
        }
        let mut ids: Vec<SubscriptionId> = Vec::new();
        for model in models {
            self.announced.remove(&model);
            self.gapped.remove(&model);
            ids.extend(self.want.remove(&model).unwrap_or_default().into_iter().filter(|id| self.streams.remove(id)));
        }
        self.subscribed.remove(&channel);
        self.channel_attempts.remove(&channel);
        if !ids.is_empty() {
            fx.push(Fx::ModelInvalid { ids });
        }
        self.narrow(fx);
        if self.want.is_empty() {
            self.mode = Mode::Off { gap: false };
            self.flush_pending(fx);
            self.close_all(fx);
        }
    }

    pub(super) fn channel_resubscribe(&mut self, connection: ConnectionId, channel: Channel, fx: &mut Vec<Fx>) {
        if self.mode != Mode::Live || self.active_connection() != Some(connection) || !self.subscribed.contains(&channel) || self.confirmed.contains(&channel) {
            return;
        }
        if let Some(found) = self.active.as_mut() {
            Self::seal(found, Outgoing::Subscribe { channel }, fx);
        }
    }

    pub(super) fn channel_lost(&mut self, connection: ConnectionId, dropped: Option<BTreeMap<String, u64>>, fx: &mut Vec<Fx>) {
        let current = match self.mode {
            Mode::Opening { .. } => self.opening_connection() == Some(connection),
            Mode::Live => self.active_connection() == Some(connection),
            _ => false,
        };
        if !current {
            return;
        }
        if self.mode == Mode::Live {
            self.flush_pending(fx);
        }
        match dropped {
            Some(counts) => {
                let models: Vec<String> = counts.keys().filter(|model| self.want.contains_key(*model)).cloned().collect();
                for model in &models {
                    self.lose(model, counts[model], fx);
                }
                Self::stale_all(&models, fx);
            }
            None => self.gap_all(fx),
        }
    }

    /// `Dropped` and `SubscriptionEnded` of a known connection.
    pub(super) fn dropped(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        match self.mode {
            Mode::Opening { .. } if self.opening_connection() == Some(connection) => {
                let lost = self.lose_held(None, fx);
                self.wait_resubscribe(fx);
                Self::stale_all(&lost, fx);
            }
            Mode::Live if self.active_connection() == Some(connection) => {
                self.flush_pending(fx);
                let lost = self.lose_held(None, fx);
                self.active = None;
                self.mode = Mode::Opening { gap: true };
                self.attempts = 0;
                self.open_connection(fx);
                Self::stale_all(&lost, fx);
            }
            _ => {}
        }
    }

    pub(super) fn protocol_failed(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        fx.push(Fx::Run(FeedEffect::Close { connection }));
        self.dropped(connection, fx);
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
            Frame::Complete { id } => self.channel_ended(connection, id, false, fx),
            Frame::Error { id: Some(id) } => self.channel_ended(connection, id, true, fx),
            Frame::Ended | Frame::Error { id: None } => self.dropped(connection, fx),
            Frame::Quiet => {}
        }
    }

    pub(super) fn resubscribe(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        let waiting = matches!(self.mode, Mode::Opening { .. }) && self.is_online() && self.opening_connection() == Some(connection);
        if !waiting {
            return;
        }
        if let Some(found) = self.opening.as_mut() {
            *found = Connection { id: connection, protocol: Protocol::Handshake };
        }
        fx.push(Fx::Run(FeedEffect::Connect { connection }));
        fx.push(Fx::HandshakeDeadline { connection });
    }

    pub(super) fn want_plus(&mut self, model: String, id: SubscriptionId, stream: bool, fx: &mut Vec<Fx>) {
        let new = !self.want.contains_key(&model);
        let position = self.position(&model);
        self.want.entry(model.clone()).or_default().insert(id);
        if stream {
            self.streams.insert(id);
            fx.push(Fx::Joined { id, position });
        }
        match self.mode {
            Mode::Off { gap } if self.is_online() => {
                self.mode = Mode::Opening { gap };
                self.attempts = 0;
                self.open_connection(fx);
            }
            Mode::Live => {
                self.subscribe_missing(fx);
                if new {
                    self.announce_ready(fx);
                } else if stream && self.announced.contains(&model) {
                    fx.push(Fx::ModelReady { model, id: Some(id) });
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
        if !ids.is_empty() {
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
                self.mode = Mode::Off { gap: false };
                self.flush_pending(fx);
                self.close_all(fx);
            }
            _ => {}
        }
    }

    /// `Opening` -> `Off`: `held` is lost, the connection closes.
    pub(super) fn leave(&mut self, fx: &mut Vec<Fx>) {
        self.close_all(fx);
        let lost = self.lose_held(None, fx);
        Self::stale_all(&lost, fx);
    }

    pub(super) fn online_changed(&mut self, online: bool, online_seq: u64, fx: &mut Vec<Fx>) {
        self.online = Some(online);
        if online {
            self.online_seq = online_seq;
        }
        match self.mode {
            Mode::Off { gap } if online && !self.want.is_empty() => {
                self.mode = Mode::Opening { gap };
                self.attempts = 0;
                self.open_connection(fx);
            }
            Mode::Opening { gap } if !online => {
                self.mode = Mode::Off { gap };
                self.leave(fx);
            }
            Mode::Live if !online => {
                self.mode = Mode::Off { gap: true };
                self.flush_pending(fx);
                self.close_all(fx);
            }
            _ => {}
        }
    }

    pub(super) fn models_reset(&mut self, reset: ModelsReset, fx: &mut Vec<Fx>) {
        for model in &reset.models {
            self.generation.insert(model.clone(), reset.generation);
        }
        if matches!(self.mode, Mode::Opening { .. }) {
            let lost = self.lose_held(Some(&reset.models), fx);
            Self::stale_all(&lost, fx);
        }
    }

    pub(super) fn end(&mut self, drained: bool, fx: &mut Vec<Fx>) {
        self.mode = Mode::Ended;
        self.refused = 0;
        self.close_all(fx);
        self.held.clear();
        if !self.pending.is_empty() {
            self.pending.clear();
            fx.push(Fx::CancelDebounce);
        }
        if drained && !std::mem::replace(&mut self.drained, true) {
            fx.push(Fx::Run(FeedEffect::Drained(Drained { owner: "feed" })));
        }
    }
}
