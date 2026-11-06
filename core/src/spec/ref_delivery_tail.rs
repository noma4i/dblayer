use super::*;

impl Delivery {
    pub(super) fn dead_on_open(&mut self, cause: Cause, fx: &mut Vec<Effect>) {
        self.stage = Stage::Ended;
        self.cause = cause;
        self.sink_closed = true;
        self.journal.push_back(Val::Ended(cause));
        fx.push(Effect::Available);
    }

    pub(super) fn same_rows(&mut self, read: i64, fx: &mut Vec<Effect>) {
        if self.held_state.as_ref().is_some_and(|held| held.visible_after() <= read) {
            let state = self.state_at(read);
            self.append(Val::State { revision: self.revision, state }, fx);
        }
    }

    /// D-73: the value carries the revision of the last commit that touched the stream; a read nobody asked a commit for carries what it saw.
    pub(super) fn shown_revision(&self, seen: i64) -> i64 { if self.read_revision > self.revision { self.read_revision.min(seen) } else { seen } }

    pub(super) fn live_rows(&mut self, seen: i64, rows: Vec<Row>, fx: &mut Vec<Effect>) {
        if seen <= self.revision { return; }
        if rows == self.rows { self.same_rows(seen, fx); return; }
        let state = self.state_at(seen);
        let revision = self.shown_revision(seen);
        let old = std::mem::replace(&mut self.rows, rows.clone());
        self.revision = revision;
        let unread_snapshot = self.journal.iter().any(|kept| matches!(kept, Val::Snapshot { .. }));
        let ids: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
        let Some(base) = self.acknowledged.filter(|_| !unread_snapshot) else {
            self.append(Val::Snapshot { revision, rows: ids, state }, fx);
            return;
        };
        let base_rows = if self.acked_rows.is_empty() && old.is_empty() { Vec::new() } else { self.acked_rows.clone() };
        let upserted: Vec<String> = rows.iter().filter(|row| !base_rows.contains(row)).map(|(id, _)| id.clone()).collect();
        let removed: Vec<String> = base_rows.iter().filter(|(id, _)| !rows.iter().any(|(now, _)| now == id)).map(|(id, _)| id.clone()).collect();
        if upserted.len() + removed.len() > DELTA_LIMIT {
            self.append(Val::Snapshot { revision, rows: ids, state }, fx);
        } else {
            self.append(Val::Delta { base, revision, size: rows.len(), upserted, removed, state }, fx);
        }
    }

    pub(super) fn replay(&mut self, fx: &mut Vec<Effect>) {
        for held in std::mem::take(&mut self.held_events) {
            match held {
                Held::Event(position, payload) => self.event(position, payload, fx),
                Held::Lost(first, last) => self.lost(first, last, fx),
                Held::Gap => if self.next_feed.is_none() || !self.ready { self.held_events.push(Held::Gap); } else { self.append(Val::Gap, fx); },
            }
        }
    }

    pub(super) fn event(&mut self, position: u64, payload: String, fx: &mut Vec<Effect>) {
        let Some(next) = self.next_feed.filter(|_| self.ready) else { self.held_events.push(Held::Event(position, payload)); return; };
        if position < next { return; }
        if position > next {
            let gap = position - next;
            self.append(Val::Lost { first: self.position + 1, last: self.position + gap }, fx);
            self.position += gap;
        }
        self.position += 1;
        self.append(Val::Event { position: self.position, payload }, fx);
        self.next_feed = Some(position + 1);
    }

    pub(super) fn lost(&mut self, first: u64, last: u64, fx: &mut Vec<Effect>) {
        let Some(next) = self.next_feed.filter(|_| self.ready) else { self.held_events.push(Held::Lost(first, last)); return; };
        if last < next { return; }
        let count = last - first.max(next) + 1;
        self.append(Val::Lost { first: self.position + 1, last: self.position + count }, fx);
        self.position += count;
        self.next_feed = Some(last + 1);
    }

    pub(super) fn status(&mut self, state: String, fx: &mut Vec<Effect>) {
        if self.last_status.as_ref() == Some(&state) { return; }
        self.stage = Stage::Live;
        self.status_revision += 1;
        self.last_status = Some(state.clone());
        self.append(Val::Status { revision: self.status_revision, state }, fx);
    }

    pub(super) fn retry(&mut self, attempts: u32, fx: &mut Vec<Effect>) {
        if self.last_retry == Some(attempts) { return; }
        self.stage = Stage::Live;
        self.last_retry = Some(attempts);
        self.append(Val::Retry { attempts }, fx);
    }
}
