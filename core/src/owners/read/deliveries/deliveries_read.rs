use std::collections::HashMap;

use super::*;

impl Delivery {
    pub(super) fn new_read(&mut self, revision: i64) -> Emit { self.start_read(revision.max(self.read_revision), ReadMode::Snapshot { from_revision: self.read_seen, from_position: self.read_position }) }

    fn start_read(&mut self, revision: i64, mode: ReadMode) -> Emit {
        self.read_id += 1;
        self.read_revision = revision;
        self.read_inflight = true;
        self.upload_inflight = self.view.kind == Kind::Blob;
        Emit::Run(StreamEffect::Read { read_id: self.read_id, revision, after: self.acknowledged.unwrap_or(0), view: self.view.clone(), mode })
    }

    fn after_commit(&mut self, revision: i64) -> Emit {
        let mode = match self.stage {
            DeliveryStage::Live => ReadMode::Changes { from_revision: self.read_seen, from_position: self.read_position, limit: DELTA_LIMIT },
            _ => ReadMode::Snapshot { from_revision: self.read_seen, from_position: self.read_position },
        };
        self.start_read(revision, mode)
    }

    fn whole(&self, data: ReadData) -> Option<Whole> {
        match data {
            ReadData::Overflow => None,
            ReadData::Rows { rows, replaced, total } => Some(Whole::Rows { rows, replaced, total }),
            ReadData::Count(count) => Some(Whole::Count(count)),
            ReadData::Operations(items) => Some(Whole::Operations(items)),
            ReadData::Upload { .. } => None,
            ReadData::Changes { keys, rows, replaced, total } => {
                let changed: HashMap<&str, &StreamRow> = rows.iter().map(|row| (row.id.as_str(), row)).collect();
                let held: HashMap<&str, &StreamRow> = self.rows.iter().map(|row| (row.id.as_str(), row)).collect();
                let rows = keys.iter().map(|(id, _)| changed.get(id.as_str()).or_else(|| held.get(id.as_str())).map(|row| (*row).clone())).collect::<Option<Vec<_>>>()?;
                Some(Whole::Rows { rows, replaced, total })
            }
        }
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
                    Value::Count { revision, .. } | Value::Operations { revision, .. } => self.acknowledge(*revision),
                    Value::Snapshot { revision, temporary, forgotten, .. } => {
                        let revision = *revision;
                        self.remember_temporary(temporary, forgotten);
                        self.acknowledge(revision);
                    }
                    Value::Delta(delta) => {
                        let revision = delta.revision;
                        self.remember_temporary(&delta.temporary, &delta.forgotten);
                        self.acknowledge(revision);
                    }
                    _ => {}
                }
                Some(PullAnswer::Value(Box::new(value)))
            }
            None if self.ended_given => Some(PullAnswer::Done),
            None => None,
        }
    }

    fn remember_temporary(&mut self, temporary: &[(String, String)], forgotten: &[String]) {
        for (id, temporary) in temporary {
            self.temporary.insert(id.clone(), temporary.clone());
        }
        for id in forgotten {
            self.temporary.remove(id);
        }
    }

    pub(super) fn acknowledge(&mut self, revision: i64) {
        self.acknowledged = Some(revision);
        self.acked_rows = self.rows.clone();
    }

    /// P-DLV-32: the state of every value of the stream, from the last value of the window, the read error and the rows of the stream.
    pub(super) fn delivered(&self, window: &WindowState) -> WindowState {
        let has_rows = if self.view.kind == Kind::Count { self.count.is_some_and(|count| count > 0) } else { !self.rows.is_empty() };
        let by_rows = if has_rows { WindowPhase::Loaded } else { WindowPhase::Failed };
        let total = if self.view.kind == Kind::List { self.total } else { None };
        if let Some(detail) = &self.read_error {
            return WindowState { phase: by_rows, error: Some(crate::ffi::DbError::Storage { detail: detail.clone() }), loaded: window.loaded, has_more: window.has_more, settled: true, total };
        }
        if !self.view.remote {
            return WindowState { phase: WindowPhase::Loaded, error: None, loaded: None, has_more: false, settled: true, total };
        }
        let phase = if window.error.is_some() && matches!(window.phase, WindowPhase::Loaded | WindowPhase::Failed) { by_rows } else { window.phase };
        WindowState { phase, total, ..window.clone() }
    }

    pub(super) fn push(&mut self, value: Value, fx: &mut Vec<Emit>) {
        let value = match value {
            Value::Snapshot { revision, rows, edits, temporary, forgotten, state } => Value::Snapshot { revision, rows, edits, temporary, forgotten, state: self.delivered(&state) },
            Value::Count { revision, count, state } => Value::Count { revision, count, state: self.delivered(&state) },
            Value::State { revision, state } => Value::State { revision, state: self.delivered(&state) },
            Value::Delta(mut delta) => {
                delta.state = self.delivered(&delta.state);
                Value::Delta(delta)
            }
            other => other,
        };
        if matches!(value, Value::Gap) && matches!(self.journal.back(), Some(Value::Gap)) {
            return;
        }
        let was_empty = self.journal.is_empty();
        match &value {
            Value::Snapshot { .. } | Value::Count { .. } | Value::Operations { .. } => self.journal.retain(|kept| !matches!(kept, Value::Delta(_) | Value::Snapshot { .. } | Value::Count { .. } | Value::Operations { .. } | Value::State { .. })),
            Value::Delta(_) if self.journal.iter().any(Value::is_rows) => self.journal.retain(|kept| !matches!(kept, Value::Delta(_) | Value::Snapshot { .. } | Value::Count { .. } | Value::State { .. })),
            Value::State { .. } => self.journal.retain(|kept| !matches!(kept, Value::State { .. })),
            Value::Status { .. } => self.journal.retain(|kept| !matches!(kept, Value::Status { .. })),
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
            let Some(index) = self.journal.iter().position(|kept| matches!(kept, Value::Event { .. } | Value::CableClose { .. } | Value::Lost { .. } | Value::Gap)) else { break };
            at.get_or_insert(index);
            match self.journal.remove(index) {
                Some(Value::Event { position, .. } | Value::CableClose { position, .. }) => {
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
            self.journal.insert(index, if first > last { Value::Gap } else { Value::Lost { first, last } });
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
            Kind::Blob => Emit::Run(StreamEffect::StatusInterest { plus: false, key: self.view.subject.clone() }),
            _ => Emit::InterestMinus,
        })
    }

    pub(super) fn changed(&mut self, committed: &Committed, fx: &mut Vec<Emit>) {
        if self.stage != DeliveryStage::Live {
            return;
        }
        let rows: Vec<(String, String)> = committed.rows.iter().flatten().cloned().collect();
        if committed.rows.as_ref().is_some_and(std::collections::BTreeSet::is_empty) {
            return;
        }
        self.push(Value::Changes { revision: committed.revision, rows }, fx);
    }

    pub(super) fn committed(&mut self, committed: &Committed, fx: &mut Vec<Emit>) {
        if self.view.kind == Kind::Blob {
            if self.sink_open() { fx.push(self.new_read(committed.revision)); }
            return;
        }
        if !self.view.kind.reads_rows() {
            return;
        }
        if self.stage == DeliveryStage::Live && (committed.revision, committed.position) <= (self.read_seen, self.read_position) { return; }
        if matches!(self.stage, DeliveryStage::Opening | DeliveryStage::Live) {
            fx.push(self.after_commit(committed.revision));
        }
    }

    pub(super) fn read_done(&mut self, read_id: u64, version: ReadVersion, data: ReadData, fx: &mut Vec<Emit>) {
        if !self.sink_open() { return; }
        if read_id == self.read_id { self.read_inflight = false; }
        if read_id == self.read_id && self.view.kind == Kind::Blob {
            if let ReadData::Upload { wait_reason, local_url, processing } = data { self.upload_read(Ok((wait_reason, local_url, processing)), version.revision, fx); }
            return;
        }
        if read_id != self.read_id || !self.view.kind.reads_rows() {
            return;
        }
        if self.stage == DeliveryStage::Live && (version.revision, version.position) < (self.read_seen, self.read_position) { return; }
        let Some(data) = self.whole(data) else {
            if self.stage == DeliveryStage::Live {
                fx.push(self.new_read(self.read_revision));
            }
            return;
        };
        self.settle_read(read_id, version, data, fx);
    }

    /// The view with the server ids when the data tells that a temporary id of the request was swapped and none is left.
    fn swapped_view(&self, data: &Whole) -> Option<View> {
        let deferred = self.deferred.as_ref()?;
        match data {
            Whole::Rows { .. } | Whole::Count(_) => (deferred.rebind.0)(),
            Whole::Operations(_) => None,
        }
    }

    /// The temporary ids of the request have server ids: the stream reads and watches the server rows, and its window opens once.
    fn rebind(&mut self, view: View, fx: &mut Vec<Emit>) {
        self.deferred = None;
        self.view = view;
        self.window = WindowState::idle();
        self.held_state = None;
        self.watch(fx);
        fx.push(self.new_read(self.read_revision));
    }

    pub(super) fn settle_read(&mut self, read_id: u64, version: ReadVersion, data: Whole, fx: &mut Vec<Emit>) {
        if let Some(view) = self.swapped_view(&data) {
            self.rebind(view, fx);
            return;
        }
        if self.stage == DeliveryStage::Opening && self.interested && self.view.remote && self.held_state.is_none() {
            self.pending_read = Some((read_id, version, data));
            return;
        }
        let revision = version.revision;
        (self.read_seen, self.read_position) = (revision, version.position);
        let changed = match &data {
            Whole::Rows { rows, total, .. } => *rows != self.rows || *total != self.total,
            Whole::Count(count) => self.count != Some(*count),
            Whole::Operations(items) => self.operations.as_ref() != Some(items),
        };
        let value = if self.stage == DeliveryStage::Opening {
            (revision, version.position)
        } else if changed {
            version.relevant.filter(|relevant| *relevant >= (self.revision, self.value_position)).unwrap_or((revision, version.position))
        } else {
            (self.revision, self.value_position)
        };
        self.value_position = value.1;
        if self.stage == DeliveryStage::Live && self.read_error.take().is_some() {
            let unchanged = match &data {
                Whole::Rows { rows, total, .. } => *rows == self.rows && *total == self.total,
                Whole::Count(count) => self.count == Some(*count),
                Whole::Operations(items) => self.operations.as_ref() == Some(items),
            };
            if !self.resync_pending && unchanged {
                let state = self.state_at(revision);
                self.push(Value::State { revision: self.revision, state }, fx);
            }
        }
        match (self.stage, data) {
            (DeliveryStage::Opening, Whole::Rows { rows, total, .. }) if self.view.kind != Kind::Count => {
                self.total = total;
                let missing = rows.is_empty() && self.view.kind == (Kind::Entity { fetch: Fetch::IfMissing }) && self.deferred.is_none();
                let state = self.state_at(revision);
                self.rows = rows.clone();
                self.revision = revision;
                self.stage = DeliveryStage::Live;
                let snapshot = self.snapshot(revision, rows, &[], state);
                self.push(snapshot, fx);
                if missing {
                    fx.push(Emit::Load { force: false });
                }
            }
            (DeliveryStage::Opening, Whole::Operations(items)) => {
                self.operations = Some(items.clone());
                self.revision = revision;
                self.stage = DeliveryStage::Live;
                self.push(Value::Operations { revision, items }, fx);
            }
            (DeliveryStage::Live, Whole::Operations(items)) => {
                if self.operations.as_ref() != Some(&items) {
                    let shown = value.0;
                    self.operations = Some(items.clone());
                    self.revision = shown;
                    self.push(Value::Operations { revision: shown, items }, fx);
                }
            }
            (DeliveryStage::Opening, Whole::Count(now)) if self.view.kind == Kind::Count => {
                let state = self.state_at(revision);
                self.count = Some(now);
                self.revision = revision;
                self.stage = DeliveryStage::Live;
                self.push(Value::Count { revision, count: now, state }, fx);
            }
            (DeliveryStage::Live, Whole::Rows { rows, replaced, total }) if self.resync_pending => {
                self.resync_pending = false;
                self.total = total;
                self.rows = rows.clone();
                let state = self.state_at(revision);
                self.revision = value.0;
                let snapshot = self.snapshot(value.0, rows, &replaced, state);
                self.push(snapshot, fx);
            }
            (DeliveryStage::Live, Whole::Rows { rows, replaced, total }) if self.view.kind != Kind::Count => self.live_rows(revision, value.0, rows, replaced, total, fx),
            (DeliveryStage::Live, Whole::Count(now)) if self.view.kind == Kind::Count => {
                if self.count != Some(now) {
                    let state = self.state_at(revision);
                    let shown = value.0;
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

    pub(super) fn live_rows(&mut self, seen: i64, revision: i64, rows: Vec<StreamRow>, replaced: Vec<(String, String)>, total: Option<u64>, fx: &mut Vec<Emit>) {
        if rows == self.rows && total == self.total {
            self.same_rows(seen, fx);
            return;
        }
        let state = self.state_at(seen);
        self.rows = rows.clone();
        self.total = total;
        self.revision = revision;
        let unread_snapshot = self.journal.iter().any(|kept| matches!(kept, Value::Snapshot { .. }));
        let Some(base) = self.acknowledged.filter(|_| !unread_snapshot) else {
            let snapshot = self.snapshot(revision, rows, &replaced, state);
            self.push(snapshot, fx);
            return;
        };
        match self.delta(base, revision, &rows, &replaced, state.clone()) {
            Some(delta) => self.push(Value::Delta(delta), fx),
            None => {
                let snapshot = self.snapshot(revision, rows, &replaced, state);
                self.push(snapshot, fx);
            }
        }
    }

}
