use super::*;

impl Delivery {
    pub(super) fn delta(&self, base: i64, revision: i64, rows: &[StreamRow], replaced: &[(String, String)], state: WindowState) -> Option<Delta> {
        let old: Vec<Entry> = self.acked_rows.iter().map(entry).collect::<Result<_, _>>().ok()?;
        let new: Vec<Entry> = rows.iter().map(entry).collect::<Result<_, _>>().ok()?;
        let pairs: Vec<(RowId, RowId)> = replaced.iter().map(|(from, to)| Ok((RowId::new(from).map_err(|e| e.to_string())?, RowId::new(to).map_err(|e| e.to_string())?))).collect::<Result<_, String>>().ok()?;
        let diff = Diff::between(&old[..], &new[..], &pairs);
        if diff.at.len() + diff.removed.len() > DELTA_LIMIT {
            return None;
        }
        let mut at: Vec<(String, usize)> = diff.at.iter().map(|(id, index)| (id.to_string(), *index)).collect();
        at.sort_by_key(|(_, index)| *index);
        let by_id: BTreeMap<&str, &StreamRow> = rows.iter().map(|row| (row.id.as_str(), row)).collect();
        let changed: Vec<StreamRow> = at.iter().filter_map(|(id, _)| by_id.get(id.as_str()).map(|row| (*row).clone())).collect();
        let pair = |list: &[(RowId, RowId)]| list.iter().map(|(from, to)| (from.to_string(), to.to_string())).collect::<Vec<_>>();
        Some(Delta {
            base,
            revision,
            size: u32::try_from(rows.len()).unwrap_or(u32::MAX),
            indexes: at.iter().map(|(_, index)| u32::try_from(*index).unwrap_or(u32::MAX)).collect(),
            rows: changed,
            removed: diff.removed.iter().map(ToString::to_string).collect(),
            placed: diff.placed().into_iter().map(|(id, index)| (id.to_string(), u32::try_from(index).unwrap_or(u32::MAX))).collect(),
            replaced: pair(&diff.replaced),
            renamed: pair(&diff.renamed),
            state,
        })
    }

    pub(super) fn read_failed(&mut self, read_id: u64, revision: i64, error: String, fx: &mut Vec<Emit>) {
        if read_id != self.read_id || !self.view.kind.reads_rows() {
            return;
        }
        if self.stage == DeliveryStage::Live && self.resync_pending {
            if self.resync_failed {
                self.finish(EndCause::Storage { error }, false, fx);
                return;
            }
            self.resync_failed = true;
            self.read_error = true;
            let phase = if self.rows.is_empty() { WindowPhase::Failed } else { WindowPhase::Loaded };
            let state = WindowState { phase, error: Some(crate::ffi::DbError::Database { detail: error }), loaded: self.window.loaded, has_more: self.window.has_more, settled: true, total: None };
            self.window = state.clone();
            self.push(Value::State { revision: self.revision, state }, fx);
            fx.push(self.new_read(self.revision));
            return;
        }
        match self.stage {
            DeliveryStage::Opening => {
                let state = WindowState { phase: WindowPhase::Failed, error: Some(crate::ffi::DbError::Database { detail: error }), loaded: None, has_more: false, settled: true, total: None };
                self.window = state.clone();
                self.revision = revision;
                self.rows = Vec::new();
                self.stage = DeliveryStage::Live;
                self.push(Value::Snapshot { revision, rows: Vec::new(), state }, fx);
            }
            DeliveryStage::Live if revision > self.revision => {
                self.read_error = true;
                let phase = if self.rows.is_empty() { WindowPhase::Failed } else { WindowPhase::Loaded };
                let state = WindowState { phase, error: Some(crate::ffi::DbError::Database { detail: error }), loaded: self.window.loaded, has_more: self.window.has_more, settled: true, total: None };
                self.window = state.clone();
                self.push(Value::State { revision: self.revision, state }, fx);
            }
            _ => {}
        }
    }

    pub(super) fn window_state(&mut self, state: WindowState, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Opening => {
                self.held_state = Some(state);
                if let Some((read_id, revision, data)) = self.pending_read.take() {
                    self.read_done(read_id, revision, data, fx);
                }
            }
            DeliveryStage::Live if self.view.kind.reads_rows() => {
                let visible = visible_after(&state);
                if visible > self.read_seen {
                    self.held_state = Some(state);
                    fx.push(self.new_read(visible));
                } else if state != self.window {
                    self.window = state.clone();
                    self.held_state = None;
                    self.push(Value::State { revision: self.revision, state }, fx);
                }
            }
            _ => {}
        }
    }

    pub(super) fn events_open(&self) -> bool { self.stage == DeliveryStage::Live && self.view.kind == Kind::Events }

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
            self.finish(EndCause::Invalid, false, fx);
        }
    }

    pub(super) fn status(&mut self, blob: BlobStatus, fx: &mut Vec<Emit>) {
        if self.view.kind != Kind::Blob || !self.sink_open() || self.blob.as_ref() == Some(&blob) {
            return;
        }
        self.stage = DeliveryStage::Live;
        self.status_revision += 1;
        self.blob = Some(blob.clone());
        self.push(Value::Status { revision: self.status_revision, blob }, fx);
    }

    pub(super) fn retry_state(&mut self, retry: RetryStatus, fx: &mut Vec<Emit>) {
        if self.view.kind != Kind::RetryStatus || !self.sink_open() || self.retry.as_ref() == Some(&retry) {
            return;
        }
        self.stage = DeliveryStage::Live;
        self.retry = Some(retry.clone());
        self.push(Value::Retry(retry), fx);
    }

    pub(super) fn models_reset(&mut self, reset: &ModelsReset, fx: &mut Vec<Emit>) {
        if !self.view.kind.reads_rows() || !reset.models.contains(&self.view.model) {
            return;
        }
        match self.stage {
            DeliveryStage::Opening => fx.push(self.new_read(self.read_revision)),
            DeliveryStage::Live => fx.push(self.new_read(self.revision)),
            DeliveryStage::Cancelling | DeliveryStage::Ended => {}
        }
    }

    pub(super) fn resync(&mut self, fx: &mut Vec<Emit>) {
        if self.stage != DeliveryStage::Live || !matches!(self.view.kind, Kind::List | Kind::Entity { .. }) { return; }
        self.acknowledged = None;
        self.resync_failed = false;
        self.resync_pending = true;
        self.journal.retain(|value| !matches!(value, Value::Snapshot { .. } | Value::Delta(_) | Value::State { .. }));
        fx.push(self.new_read(self.revision));
    }

    pub(super) fn pull(&mut self, outcome: OutcomeId, fx: &mut Vec<Emit>) {
        if self.waiting.is_some() {
            fx.push(Emit::Run(StreamEffect::Answer { outcome, answer: PullAnswer::Invalid }));
            return;
        }
        match self.take_first() {
            Some(answer) => fx.push(Emit::Run(StreamEffect::Answer { outcome, answer })),
            None if self.stage == DeliveryStage::Ended => fx.push(Emit::Run(StreamEffect::Answer { outcome, answer: PullAnswer::Done })),
            None => self.waiting = Some(outcome),
        }
    }

    pub(super) fn cancel(&mut self, reply: ReplyId, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Opening | DeliveryStage::Live => {
                fx.extend(self.leave());
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

    pub(super) fn release(&mut self, fx: &mut Vec<Emit>) {
        match self.stage {
            DeliveryStage::Opening | DeliveryStage::Live => {
                fx.extend(self.leave());
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
        fx.extend(self.leave());
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
            None => fx.push(Emit::Run(StreamEffect::Available)),
        }
        fx.push(Emit::Run(StreamEffect::CloseSink));
    }

    pub(super) fn epoch_ended(&mut self, ended: &EpochEnded, fx: &mut Vec<Emit>) {
        if ended.epoch != self.epoch {
            return;
        }
        let cause = if ended.cause == Cause::Close { EndCause::Closed } else { EndCause::SessionEnded };
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
