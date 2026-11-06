use super::*;

impl Delivery {
    pub(super) fn new_read(&mut self, revision: i64) -> Emit {
        self.read_id += 1;
        self.read_revision = revision;
        Emit::Run(StreamEffect::Read { read_id: self.read_id, revision, after: self.acknowledged.unwrap_or(0), view: self.view.clone() })
    }

    pub(super) fn sink_open(&self) -> bool { matches!(self.stage, DeliveryStage::Opening | DeliveryStage::Live) }

    pub(super) fn take_first(&mut self) -> Option<PullAnswer> {
        match self.journal.pop_front() {
            Some(value) => {
                match &value {
                    Value::Ended(_) => {
                        self.ended_given = true;
                        self.journal.clear();
                    }
                    Value::Snapshot { revision, .. } | Value::Count { revision, .. } => self.acknowledge(*revision),
                    Value::Delta(delta) => self.acknowledge(delta.revision),
                    _ => {}
                }
                Some(PullAnswer::Value(Box::new(value)))
            }
            None if self.ended_given => Some(PullAnswer::Done),
            None => None,
        }
    }

    pub(super) fn acknowledge(&mut self, revision: i64) {
        self.acknowledged = Some(revision);
        self.acked_rows = self.rows.clone();
    }

    /// `Failed` only without rows, an error with rows is `Loaded` with the error: the rows of the snapshot decide, not the window.
    pub(super) fn shown(&self, mut state: WindowState) -> WindowState {
        if state.error.is_some() && matches!(state.phase, WindowPhase::Loaded | WindowPhase::Failed) {
            let has_rows = if self.view.kind == Kind::Count { self.count.is_some_and(|count| count > 0) } else { !self.rows.is_empty() };
            state.phase = if has_rows { WindowPhase::Loaded } else { WindowPhase::Failed };
        }
        state.total = if self.view.kind == Kind::List { self.total } else { None };
        state
    }

    pub(super) fn push(&mut self, value: Value, fx: &mut Vec<Emit>) {
        let value = match value {
            Value::Snapshot { revision, rows, state } => Value::Snapshot { revision, rows, state: self.shown(state) },
            Value::Count { revision, count, state } => Value::Count { revision, count, state: self.shown(state) },
            Value::State { revision, state } => Value::State { revision, state: self.shown(state) },
            Value::Delta(mut delta) => {
                delta.state = self.shown(delta.state);
                Value::Delta(delta)
            }
            other => other,
        };
        let was_empty = self.journal.is_empty();
        match &value {
            Value::Snapshot { .. } | Value::Count { .. } => self.journal.retain(|kept| !matches!(kept, Value::Delta(_) | Value::Snapshot { .. } | Value::Count { .. } | Value::State { .. })),
            Value::Delta(_) if self.journal.iter().any(Value::is_rows) => self.journal.retain(|kept| !matches!(kept, Value::Delta(_) | Value::Snapshot { .. } | Value::Count { .. } | Value::State { .. })),
            Value::State { .. } => self.journal.retain(|kept| !matches!(kept, Value::State { .. })),
            Value::Status { .. } => self.journal.retain(|kept| !matches!(kept, Value::Status { .. })),
            Value::Retry(_) => self.journal.retain(|kept| !matches!(kept, Value::Retry(_))),
            _ => {}
        }
        self.journal.push_back(value);
        if self.journal.len() > JOURNAL_LIMIT {
            self.compact();
        }
        if let Some(outcome) = self.waiting.take() {
            let answer = self.take_first().unwrap_or(PullAnswer::Done);
            fx.push(Emit::Run(StreamEffect::Answer { outcome, answer }));
        } else if was_empty && self.sink_open() {
            fx.push(Emit::Run(StreamEffect::Available));
        }
    }

    pub(super) fn compact(&mut self) {
        let (mut first, mut last, mut at) = (u64::MAX, 0u64, None);
        while self.journal.len() > JOURNAL_LIMIT - 1 {
            let Some(index) = self.journal.iter().position(|kept| matches!(kept, Value::Event { .. } | Value::Lost { .. })) else { break };
            at.get_or_insert(index);
            match self.journal.remove(index) {
                Some(Value::Event { position, .. }) => {
                    first = first.min(position);
                    last = last.max(position);
                }
                Some(Value::Lost { first: from, last: to }) => {
                    first = first.min(from);
                    last = last.max(to);
                }
                _ => {}
            }
        }
        if let Some(index) = at {
            self.journal.insert(index, Value::Lost { first, last });
        }
    }

    pub(super) fn state_at(&mut self, revision: i64) -> WindowState {
        if self.held_state.as_ref().is_some_and(|held| visible_after(held) <= revision) {
            if let Some(held) = self.held_state.take() {
                self.window = held;
            }
        }
        self.window.clone()
    }

    pub(super) fn unwant(&mut self) -> Option<Emit> { std::mem::take(&mut self.wants).then_some(Emit::WantMinus) }

    pub(super) fn leave(&mut self) -> Option<Emit> {
        if !std::mem::take(&mut self.interested) {
            return None;
        }
        Some(match self.view.kind {
            Kind::Events => Emit::WantMinus,
            Kind::Blob | Kind::RetryStatus => Emit::Run(StreamEffect::StatusInterest { plus: false }),
            _ => Emit::InterestMinus,
        })
    }

    pub(super) fn touches(&self, committed: &Committed) -> bool { committed.models == Models::All || committed.tables.iter().any(|table| self.view.tables.contains(table)) }

    pub(super) fn committed(&mut self, committed: &Committed, fx: &mut Vec<Emit>) {
        if !self.view.kind.reads_rows() || !self.touches(committed) {
            return;
        }
        if self.stage == DeliveryStage::Live && (committed.revision, committed.position) <= (self.read_seen, self.read_position) { return; }
        if matches!(self.stage, DeliveryStage::Opening | DeliveryStage::Live) {
            self.reading_position = committed.position;
            fx.push(self.new_read(committed.revision));
        }
    }

    pub(super) fn read_done(&mut self, read_id: u64, revision: i64, data: ReadData, fx: &mut Vec<Emit>) {
        if read_id != self.read_id || !self.view.kind.reads_rows() {
            return;
        }
        if self.stage == DeliveryStage::Opening && self.interested && self.view.remote && self.held_state.is_none() {
            self.pending_read = Some((read_id, revision, data));
            return;
        }
        self.read_position = if revision > self.read_seen { self.reading_position } else { self.read_position.max(self.reading_position) };
        self.read_seen = self.read_seen.max(revision);
        if self.stage == DeliveryStage::Live && (revision > self.revision || self.resync_pending) && self.read_error {
            self.read_error = false;
            self.window.error = None;
            self.window.phase = WindowPhase::Loaded;
            if !self.resync_pending && matches!(&data, ReadData::Rows { rows, .. } if *rows == self.rows) {
                self.push(Value::State { revision: self.revision, state: self.window.clone() }, fx);
            }
        }
        match (self.stage, data) {
            (DeliveryStage::Opening, ReadData::Rows { rows, total, .. }) if self.view.kind != Kind::Count => {
                self.total = total;
                let missing = rows.is_empty() && self.view.kind == (Kind::Entity { fetch: Fetch::IfMissing });
                let state = self.state_at(revision);
                self.rows = rows.clone();
                self.revision = revision;
                self.stage = DeliveryStage::Live;
                self.push(Value::Snapshot { revision, rows, state }, fx);
                if missing {
                    fx.push(Emit::Load { force: false });
                }
            }
            (DeliveryStage::Opening, ReadData::Count(now)) if self.view.kind == Kind::Count => {
                let state = self.state_at(revision);
                self.count = Some(now);
                self.revision = revision;
                self.stage = DeliveryStage::Live;
                self.push(Value::Count { revision, count: now, state }, fx);
            }
            (DeliveryStage::Live, ReadData::Rows { rows, total, .. }) if self.resync_pending => {
                self.resync_pending = false;
                self.total = total;
                self.rows = rows.clone();
                self.revision = revision;
                let state = self.state_at(revision);
                self.push(Value::Snapshot { revision, rows, state }, fx);
            }
            (DeliveryStage::Live, ReadData::Rows { rows, replaced, total }) if self.view.kind != Kind::Count => self.live_rows(revision, rows, replaced, total, fx),
            (DeliveryStage::Live, ReadData::Count(now)) if self.view.kind == Kind::Count && revision > self.revision => {
                if self.count != Some(now) {
                    let state = self.state_at(revision);
                    let shown = self.shown_revision(revision);
                    self.count = Some(now);
                    self.revision = shown;
                    self.push(Value::Count { revision: shown, count: now, state }, fx);
                } else {
                    self.same_rows(revision, fx);
                }
            }
            _ => {}
        }
    }

    pub(super) fn same_rows(&mut self, read: i64, fx: &mut Vec<Emit>) {
        if self.held_state.as_ref().is_some_and(|held| visible_after(held) <= read) {
            let state = self.state_at(read);
            self.push(Value::State { revision: self.revision, state }, fx);
        }
    }

    /// The revision a value carries: the last commit that touched the stream, not the head of the database at the read (D-73). A read that no commit asked for (a reset of models) carries the revision it saw.
    pub(super) fn shown_revision(&self, seen: i64) -> i64 { if self.read_revision > self.revision { self.read_revision.min(seen) } else { seen } }

    pub(super) fn live_rows(&mut self, seen: i64, rows: Vec<StreamRow>, replaced: Vec<(String, String)>, total: Option<u64>, fx: &mut Vec<Emit>) {
        if seen < self.revision || (seen == self.revision && self.reading_position <= self.shown_position) {
            return;
        }
        self.shown_position = if seen > self.revision { self.reading_position } else { self.reading_position.max(self.shown_position) };
        let revision = self.shown_revision(seen);
        if rows == self.rows {
            self.same_rows(seen, fx);
            return;
        }
        let state = self.state_at(seen);
        self.rows = rows.clone();
        self.total = total;
        self.revision = revision;
        let unread_snapshot = self.journal.iter().any(|kept| matches!(kept, Value::Snapshot { .. }));
        let Some(base) = self.acknowledged.filter(|_| !unread_snapshot) else {
            self.push(Value::Snapshot { revision, rows, state }, fx);
            return;
        };
        match self.delta(base, revision, &rows, &replaced, state.clone()) {
            Some(delta) => self.push(Value::Delta(delta), fx),
            None => self.push(Value::Snapshot { revision, rows, state }, fx),
        }
    }

}
