use super::*;

impl Feed {
    pub(super) fn want_plus(&mut self, model: &str, id: u64, stream: bool) -> Vec<Effect> {
        self.want.entry(model.to_string()).or_default().insert(id);
        let mut effects = Vec::new();
        if stream {
            self.streams.insert(id);
            effects.push(Effect::Joined(id));
        }
        match self.mode {
            Mode::Off { gap } if self.online() => {
                self.mode = Mode::Opening { gap };
                self.opening = Some(Proto::Handshake);
                effects.push(Effect::Connect);
            }
            Mode::Live if stream => effects.push(Effect::ModelReady(id)),
            _ => {}
        }
        effects
    }

    pub(super) fn want_minus(&mut self, model: &str, id: u64) -> Vec<Effect> {
        let mut effects = Vec::new();
        let Some(ids) = self.want.get_mut(model) else { return effects };
        if !ids.remove(&id) {
            return effects;
        }
        self.streams.remove(&id);
        if !ids.is_empty() {
            return effects;
        }
        self.want.remove(model);
        if !self.want.is_empty() {
            return effects;
        }
        match self.mode {
            Mode::Opening { gap } => {
                self.mode = Mode::Off { gap };
                self.close_all(&mut effects);
                let lost = self.lose_held(None);
                Self::stale(&lost, &mut effects);
            }
            Mode::Live => {
                if !self.pending.is_empty() {
                    self.commit_pending(&mut effects);
                }
                self.drain_all(Then::Off { gap: false }, &mut effects);
            }
            _ => {}
        }
        effects
    }

    /// The protocol of the active connection failed: it drains, the feed opens another one after the drain.
    pub(super) fn fail_live(&mut self, effects: &mut Vec<Effect>) {
        if !self.pending.is_empty() {
            self.commit_pending(effects);
        }
        self.active = None;
        self.mode = Mode::Draining;
        self.then = Then::Reopen;
    }

    /// `Dropped` of the connection `to` in `Opening` or `Live`.
    pub(super) fn dropped(&mut self, to: To) -> Vec<Effect> {
        let mut effects = Vec::new();
        match (self.mode, to) {
            (Mode::Opening { .. }, To::Opening) => {
                let lost = self.lose_held(None);
                self.opening = Some(Proto::Handshake);
                effects.push(Effect::ScheduleResubscribe);
                Self::stale(&lost, &mut effects);
            }
            (Mode::Live, To::Active) => {
                if !self.pending.is_empty() {
                    self.commit_pending(&mut effects);
                }
                self.active = None;
                self.mode = Mode::Opening { gap: true };
                self.opening = Some(Proto::Handshake);
                effects.push(Effect::Connect);
            }
            _ => {}
        }
        effects
    }

    /// `Event` of the connection `to`.
    pub(super) fn event(&mut self, to: To, model: &str, payload: &str, effects: &mut Vec<Effect>) {
        if !self.want.contains_key(model) {
            return;
        }
        match (self.mode, to) {
            (Mode::Opening { .. }, To::Opening) => {
                let event = self.accept(model, payload);
                self.held.push(event);
            }
            (Mode::Live, To::Active) => {
                let event = self.accept(model, payload);
                self.seat(vec![event], effects);
            }
            _ => {}
        }
    }

    pub(super) fn ready(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        match self.mode {
            Mode::Opening { gap } if self.opening.is_some() => {
                self.active = self.opening.take();
                self.mode = Mode::Live;
                effects.push(Effect::SealSubscribe);
                if gap {
                    self.gap_all(&mut effects);
                }
                self.ready_all(&mut effects);
                let held = std::mem::take(&mut self.held);
                self.seat(held, &mut effects);
            }
            _ => {}
        }
        effects
    }

    pub(super) fn online_changed(&mut self, on: bool) -> Vec<Effect> {
        self.online = Some(on);
        let mut effects = Vec::new();
        match self.mode {
            Mode::Off { gap } if on && !self.want.is_empty() => {
                self.mode = Mode::Opening { gap };
                self.opening = Some(Proto::Handshake);
                effects.push(Effect::Connect);
            }
            Mode::Opening { gap } if !on => {
                self.mode = Mode::Off { gap };
                self.close_all(&mut effects);
                let lost = self.lose_held(None);
                Self::stale(&lost, &mut effects);
            }
            Mode::Live if !on => {
                if !self.pending.is_empty() {
                    self.commit_pending(&mut effects);
                }
                self.drain_all(Then::Off { gap: true }, &mut effects);
            }
            _ => {}
        }
        effects
    }
}
