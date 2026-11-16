use super::*;

impl Delivery {
    /// The steps from the rows of the last value the reader took to `rows`.
    fn steps(&self, rows: &[StreamRow], replaced: &[(String, String)]) -> Result<Steps, String> {
        let old: Vec<Entry> = self.acked_rows.iter().map(entry).collect::<Result<_, _>>()?;
        let new: Vec<Entry> = rows.iter().map(entry).collect::<Result<_, _>>()?;
        let pairs: Vec<(RowId, RowId)> = replaced.iter().map(|(from, to)| Ok((RowId::new(from).map_err(|e| e.to_string())?, RowId::new(to).map_err(|e| e.to_string())?))).collect::<Result<_, String>>()?;
        let diff = Diff::between(&old[..], &new[..], &pairs);
        let (temporary, forgotten) = self.temporary_ids(&diff);
        let edits = edits(&self.acked_rows, rows, &diff, &temporary.iter().cloned().collect());
        Ok(Steps { edits, temporary, forgotten, touched: diff.at.len() + diff.removed.len() })
    }

    pub(super) fn delta(&self, base: i64, revision: i64, rows: &[StreamRow], replaced: &[(String, String)], state: WindowState) -> Result<Option<Delta>, String> {
        let steps = self.steps(rows, replaced)?;
        if steps.touched > DELTA_LIMIT { return Ok(None); }
        Ok(Some(Delta {
            base,
            revision,
            size: u32::try_from(rows.len()).unwrap_or(u32::MAX),
            edits: steps.edits,
            temporary: steps.temporary,
            forgotten: steps.forgotten,
            state,
        }))
    }

    /// The snapshot of `rows` goes to the journal; steps that do not parse end the stream `Internal`.
    pub(super) fn push_snapshot(&mut self, revision: i64, rows: Vec<StreamRow>, replaced: &[(String, String)], state: WindowState, fx: &mut Vec<Emit>) {
        match self.steps(&rows, replaced) {
            Ok(steps) => self.push(Value::Snapshot { revision, rows, edits: steps.edits, temporary: steps.temporary, forgotten: steps.forgotten, state }, fx),
            Err(detail) => self.finish(EndCause::Internal { detail }, false, fx),
        }
    }

    fn temporary_ids(&self, diff: &Diff) -> (Vec<(String, String)>, Vec<String>) {
        let mut ids = self.temporary.clone();
        let pairs: Vec<(String, String)> = diff.replaced.iter().chain(&diff.renamed).map(|(from, to)| (from.to_string(), to.to_string())).collect();
        for (from, to) in &pairs {
            let temporary = ids.get(from).cloned().unwrap_or_else(|| from.clone());
            ids.insert(to.clone(), temporary);
            ids.remove(from);
        }
        let set = pairs.iter().filter_map(|(_, to)| ids.get(to).map(|temporary| (to.clone(), temporary.clone()))).collect();
        let gone = pairs.iter().filter(|(from, _)| !ids.contains_key(from)).map(|(from, _)| from.clone()).collect();
        (set, gone)
    }

    pub(super) fn read_failed(&mut self, read_id: u64, revision: i64, error: String, fx: &mut Vec<Emit>) {
        if !self.sink_open() { return; }
        if read_id == self.read_id { self.read_inflight = false; }
        if read_id == self.read_id && self.view.kind == Kind::Blob {
            self.upload_read(Err(error), revision, fx);
            return;
        }
        if read_id != self.read_id || !self.view.kind.reads_rows() {
            return;
        }
        if self.view.kind == Kind::Operations {
            self.finish(EndCause::Storage { error }, false, fx);
            return;
        }
        if self.stage == DeliveryStage::Live && self.resync_pending {
            if self.resync_failed {
                self.finish(EndCause::Storage { error }, false, fx);
                return;
            }
            self.resync_failed = true;
            self.read_error = Some(error);
            let state = self.window.clone();
            self.push(Value::State { revision: self.revision, state }, fx);
            fx.push(self.new_read(self.revision));
            return;
        }
        match self.stage {
            DeliveryStage::Opening => {
                let state = self.window.clone();
                self.revision = revision;
                self.rows = Vec::new();
                self.stage = DeliveryStage::Live;
                self.read_error = Some(error);
                if self.view.kind == Kind::Count {
                    self.count = Some(0);
                    self.push(Value::Count { revision, count: 0, state }, fx);
                } else {
                    self.push_snapshot(revision, Vec::new(), &[], state, fx);
                }
            }
            DeliveryStage::Live => {
                self.read_error = Some(error);
                let state = self.window.clone();
                self.push(Value::State { revision: self.revision, state }, fx);
            }
            _ => {}
        }
    }

    pub(super) fn window_state(&mut self, state: WindowState, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Opening => {
                self.held_state = Some(state);
                if let Some((read_id, version, data)) = self.pending_read.take().filter(|(read_id, ..)| *read_id == self.read_id) {
                    self.settle_read(read_id, version, data, fx);
                    self.settle_debts(fx);
                }
            }
            DeliveryStage::Live if self.view.kind.reads_rows() => {
                let visible = visible_after(&state);
                if visible > self.read_seen {
                    self.held_state = Some(state);
                    self.catch_up(fx);
                } else if state != self.window {
                    let before = self.delivered(&self.window);
                    self.window = state.clone();
                    self.held_state = None;
                    if self.delivered(&state) != before { self.push(Value::State { revision: self.revision, state }, fx); }
                }
            }
            _ => {}
        }
    }

    pub(super) fn catch_up(&mut self, fx: &mut Vec<Emit>) {
        if self.stage != DeliveryStage::Live || !self.view.kind.reads_rows() { return; }
        let Some(visible) = self.held_state.as_ref().map(visible_after).filter(|visible| *visible > self.read_seen) else { return };
        if !self.read_inflight || self.read_revision < visible { fx.push(self.new_read(visible)); }
    }

    pub(super) fn events_open(&self) -> bool { self.stage == DeliveryStage::Live && self.view.kind == Kind::Events }

    pub(super) fn cable_closed(&mut self, event: crate::owners::read::feed::CableClose, fx: &mut Vec<Emit>) {
        if self.stage != DeliveryStage::Live || self.view.kind != Kind::CableCloses { return; }
        self.position += 1;
        self.push(Value::CableClose { position: self.position, event }, fx);
    }

    pub(super) fn requested(&mut self, record: crate::transport::RequestRecord, fx: &mut Vec<Emit>) {
        if self.stage != DeliveryStage::Live || self.view.kind != Kind::Requests { return; }
        self.position += 1;
        self.push(Value::Event { position: self.position, payload: serde_json::json!({ "operation": record.operation, "variables_json": record.variables_json, "reason": format!("{:?}", record.reason) }).to_string() }, fx);
    }

    pub(super) fn joined(&mut self, position: u64, fx: &mut Vec<Emit>) {
        if self.events_open() && self.next_feed.is_none() {
            self.next_feed = Some(position + 1);
            self.replay(fx);
        }
    }

    pub(super) fn model_ready(&mut self, fx: &mut Vec<Emit>) {
        if self.events_open() && !self.ready {
            self.ready = true;
            self.push(Value::Ready, fx);
            self.replay(fx);
        }
    }

    pub(super) fn replay(&mut self, fx: &mut Vec<Emit>) {
        for held in std::mem::take(&mut self.held_events) {
            match held {
                Held::Event { position, payload } => self.model_event(position, payload, fx),
                Held::Lost { first, last } => self.model_lost(first, last, fx),
                Held::Gap => self.model_gap(fx),
            }
        }
    }

    pub(super) fn holds(&self) -> bool { self.next_feed.is_none() || !self.ready }

    pub(super) fn model_event(&mut self, position: u64, payload: String, fx: &mut Vec<Emit>) {
        if !self.events_open() {
            return;
        }
        if self.holds() {
            self.held_events.push(Held::Event { position, payload });
            return;
        }
        let next = self.next_feed.unwrap_or(0);
        if position < next {
            return;
        }
        if position > next {
            let gap = position - next;
            self.push(Value::Lost { first: self.position + 1, last: self.position + gap }, fx);
            self.position += gap;
        }
        self.position += 1;
        self.push(Value::Event { position: self.position, payload }, fx);
        self.next_feed = Some(position + 1);
    }

    pub(super) fn model_lost(&mut self, first: u64, last: u64, fx: &mut Vec<Emit>) {
        if !self.events_open() {
            return;
        }
        if self.holds() {
            self.held_events.push(Held::Lost { first, last });
            return;
        }
        let next = self.next_feed.unwrap_or(0);
        if last < next {
            return;
        }
        let count = last - first.max(next) + 1;
        self.push(Value::Lost { first: self.position + 1, last: self.position + count }, fx);
        self.position += count;
        self.next_feed = Some(last + 1);
    }

    pub(super) fn model_gap(&mut self, fx: &mut Vec<Emit>) {
        if !self.events_open() {
            return;
        }
        if self.holds() {
            self.held_events.push(Held::Gap);
            return;
        }
        self.push(Value::Gap, fx);
    }

    pub(super) fn model_invalid(&mut self, fx: &mut Vec<Emit>) {
        if self.events_open() {
            self.finish(EndCause::Invalid { detail: "the stream request is invalid".into() }, false, fx);
        }
    }

    pub(super) fn status(&mut self, blob: BlobStatus, revision: i64, fx: &mut Vec<Emit>) {
        if self.view.kind != Kind::Blob || !self.sink_open() {
            return;
        }
        let changed_reason = self.upload_row.as_ref().and_then(|row| row.as_ref().ok()).is_none_or(|(reason, _)| *reason != blob.wait_reason);
        self.transfer = Some(blob);
        self.status_revision = self.status_revision.max(revision);
        if changed_reason { fx.push(self.new_read(self.read_revision)); } else if !self.upload_inflight { self.show_status(fx); }
    }

    pub(super) fn upload_read(&mut self, row: UploadRowRead, revision: i64, fx: &mut Vec<Emit>) {
        self.upload_row = Some(row);
        self.upload_inflight = false;
        self.status_revision = self.status_revision.max(revision);
        self.show_status(fx);
    }

    /// The value of a `Blob` stream: the state of the transfer with the copy URL of its row, pushed when it differs from the last one.
    fn show_status(&mut self, fx: &mut Vec<Emit>) {
        let (Some(transfer), Some(row)) = (&self.transfer, &self.upload_row) else { return };
        let blob = match row {
            Ok((wait_reason, local_url)) => BlobStatus { wait_reason: *wait_reason, local_url: local_url.clone(), ..transfer.clone() },
            Err(error) => BlobStatus { state: "failed".into(), wait_reason: None, progress: 0, error: Some(error.clone()), local_url: None },
        };
        let blob = if matches!(blob.state.as_str(), "gone" | "failed") && self.blob.is_some() { BlobStatus { progress: self.blob.as_ref().map_or(0, |last| last.progress), ..blob } } else { blob };
        if !self.sink_open() || self.blob.as_ref() == Some(&blob) {
            return;
        }
        self.stage = DeliveryStage::Live;
        self.blob = Some(blob.clone());
        self.push(Value::Status { revision: self.status_revision, blob }, fx);
    }

    pub(super) fn resync(&mut self, fx: &mut Vec<Emit>) {
        if self.stage != DeliveryStage::Live || !matches!(self.view.kind, Kind::List | Kind::Entity { .. }) { return; }
        self.acknowledged = None;
        self.resync_failed = false;
        self.resync_pending = true;
        self.journal.retain(|value| !matches!(value, Value::Snapshot { .. } | Value::Delta(_) | Value::State { .. }));
        fx.push(self.new_read(self.revision));
    }

    pub(super) fn pull(&mut self, outcome: OutcomeId, reader: u64, fx: &mut Vec<Emit>) {
        if !self.claim(reader) || self.waiting.is_some() {
            fx.push(Emit::Run(StreamEffect::Answer { outcome, answer: PullAnswer::ended(EndCause::Invalid { detail: "a Pull is already waiting or the reader is not the stream reader".into() }) }));
            return;
        }
        match self.take_first() {
            Some(answer) => fx.push(Emit::Run(StreamEffect::Answer { outcome, answer })),
            None if self.stage == DeliveryStage::Ended => fx.push(Emit::Run(StreamEffect::Answer { outcome, answer: PullAnswer::Done })),
            None => self.waiting = Some(outcome),
        }
    }

    pub(super) fn pull_cancelled(&mut self, outcome: OutcomeId, fx: &mut Vec<Emit>) {
        if self.waiting != Some(outcome) { return; }
        self.waiting = None;
        fx.push(Emit::Run(StreamEffect::Answer { outcome, answer: PullAnswer::Interrupted }));
        if matches!(self.stage, DeliveryStage::Opening | DeliveryStage::Live) {
            self.leave_all(fx);
            fx.extend(self.unwant());
            self.stage = DeliveryStage::Cancelling;
            self.cause = EndCause::Cancelled;
            self.journal.clear();
            fx.push(Emit::Run(StreamEffect::CloseSink));
        }
    }

    pub(super) fn cancel(&mut self, reply: ReplyId, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Opening | DeliveryStage::Live => {
                self.leave_all(fx);
                fx.extend(self.unwant());
                self.stage = DeliveryStage::Cancelling;
                self.cause = EndCause::Cancelled;
                self.journal.clear();
                self.replies.push(reply);
                fx.push(Emit::Run(StreamEffect::CloseSink));
            }
            DeliveryStage::Cancelling => self.replies.push(reply),
            DeliveryStage::Ended if self.sink_closed => fx.push(Emit::Run(StreamEffect::Cancelled { reply })),
            DeliveryStage::Ended => self.replies.push(reply),
        }
    }

    pub(super) fn claim(&mut self, reader: u64) -> bool { *self.reader.get_or_insert(reader) == reader }

    pub(super) fn release(&mut self, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Opening | DeliveryStage::Live => {
                self.leave_all(fx);
                fx.extend(self.unwant());
                self.stage = DeliveryStage::Cancelling;
                self.cause = EndCause::Cancelled;
                self.journal.clear();
                self.released = true;
                fx.push(Emit::Run(StreamEffect::CloseSink));
            }
            DeliveryStage::Cancelling | DeliveryStage::Ended => self.released = true,
        }
    }

    pub(super) fn finish(&mut self, cause: EndCause, closing: bool, fx: &mut Vec<Emit>) {
        self.leave_all(fx);
                fx.extend(self.unwant());
        self.stage = DeliveryStage::Ended;
        self.cause = cause.clone();
        self.closing = closing;
        self.journal.clear();
        self.journal.push_back(Value::Ended(cause));
        match self.waiting.take() {
            Some(outcome) => {
                let answer = self.take_first().unwrap_or(PullAnswer::Done);
                fx.push(Emit::Run(StreamEffect::Answer { outcome, answer }));
            }
            None => {}
        }
        fx.push(Emit::Run(StreamEffect::CloseSink));
    }

    pub(super) fn epoch_ended(&mut self, ended: &EpochEnded, fx: &mut Vec<Emit>) {
        if ended.epoch != self.epoch {
            return;
        }
        let cause = EndCause::of(ended.cause);
        match self.stage {
            DeliveryStage::Opening | DeliveryStage::Live => self.finish(cause, false, fx),
            DeliveryStage::Cancelling => self.cause = cause,
            DeliveryStage::Ended => {}
        }
    }

    pub(super) fn closing(&mut self, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Opening | DeliveryStage::Live => self.finish(EndCause::Closed, true, fx),
            DeliveryStage::Cancelling => {
                self.cause = EndCause::Closed;
                self.closing = true;
            }
            DeliveryStage::Ended => self.closing = true,
        }
    }

    pub(super) fn sink_closed(&mut self, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Cancelling => {
                for reply in self.replies.drain(..) {
                    fx.push(Emit::Run(StreamEffect::Cancelled { reply }));
                }
                self.stage = DeliveryStage::Ended;
                self.sink_closed = true;
                if self.released {
                    if let Some(outcome) = self.waiting.take() { fx.push(Emit::Run(StreamEffect::Answer { outcome, answer: PullAnswer::Done })); }
                    return;
                }
                self.journal.clear();
                self.journal.push_back(Value::Ended(self.cause.clone()));
                if let Some(outcome) = self.waiting.take() {
                    let answer = self.take_first().unwrap_or(PullAnswer::Done);
                    fx.push(Emit::Run(StreamEffect::Answer { outcome, answer }));
                }
            }
            DeliveryStage::Ended if !self.sink_closed => {
                self.sink_closed = true;
                for reply in self.replies.drain(..) {
                    fx.push(Emit::Run(StreamEffect::Cancelled { reply }));
                }
            }
            _ => {}
        }
    }

    pub(super) fn gone(&self) -> bool { self.stage == DeliveryStage::Ended && self.released && self.sink_closed }

    pub(super) fn settled(&self) -> bool { self.stage == DeliveryStage::Ended && self.sink_closed }
}

struct Steps {
    edits: Vec<Edit>,
    temporary: Vec<(String, String)>,
    forgotten: Vec<String>,
    touched: usize,
}

fn at(index: usize) -> u32 { u32::try_from(index).unwrap_or(u32::MAX) }

fn edits(old: &[StreamRow], new: &[StreamRow], diff: &Diff, temporary: &BTreeMap<String, String>) -> Vec<Edit> {
    let alias: BTreeMap<&RowId, &RowId> = diff.replaced.iter().map(|(from, to)| (from, to)).collect();
    let mut work: Vec<String> = old.iter().map(|row| RowId::new(&row.id).ok().and_then(|id| alias.get(&id).map(ToString::to_string)).unwrap_or_else(|| row.id.clone())).collect();
    let mut steps = Vec::new();
    let gone: BTreeSet<String> = diff.removed.iter().map(ToString::to_string).collect();
    let leaving: Vec<usize> = (0..work.len()).rev().filter(|index| gone.contains(&work[*index])).collect();
    for index in leaving {
        steps.push(Edit::Remove { index: at(index), id: work.remove(index) });
    }
    let placing: BTreeSet<String> = diff.inserted.iter().chain(&diff.moved).map(ToString::to_string).collect();
    for (position, row) in new.iter().enumerate().filter(|(_, row)| placing.contains(&row.id)) {
        let from = work.iter().position(|id| *id == row.id);
        if let Some(from) = from {
            work.remove(from);
        }
        let to = match position.checked_sub(1) {
            Some(before) => work.iter().position(|id| *id == new[before].id).map_or(0, |before| before + 1),
            None => 0,
        };
        work.insert(to, row.id.clone());
        match from {
            Some(from) if from != to => steps.push(Edit::Move { from: at(from), to: at(to) }),
            Some(_) => {}
            None => steps.push(Edit::Insert { index: at(to), row: row.clone() }),
        }
    }
    let rewritten: BTreeSet<String> = diff.updated.iter().chain(diff.replaced.iter().map(|(_, to)| to)).map(ToString::to_string).collect();
    for (position, row) in new.iter().enumerate().filter(|(_, row)| rewritten.contains(&row.id)) {
        steps.push(Edit::Rewrite { index: at(position), row: row.clone(), temporary: temporary.get(&row.id).cloned() });
    }
    steps
}
