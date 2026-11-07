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
        if rows == self.rows && self.incoming_total == self.total { self.same_rows(seen, fx); return; }
        let state = self.state_at(seen);
        let revision = self.shown_revision(seen);
        self.rows = rows.clone();
        self.total = self.incoming_total;
        self.revision = revision;
        let unread_snapshot = self.journal.iter().any(|kept| matches!(kept, Val::Snapshot { .. }));
        let ids: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
        let Some(base) = self.acknowledged.filter(|_| !unread_snapshot) else {
            self.append(Val::Snapshot { revision, rows: ids, state }, fx);
            return;
        };
        let (edits, touched) = edits_between(&self.acked_rows, &rows);
        if touched > DELTA_LIMIT {
            self.append(Val::Snapshot { revision, rows: ids, state }, fx);
        } else {
            self.append(Val::Delta { base, revision, size: rows.len(), edits, state }, fx);
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

/// The steps from `old` to `new` and the number of rows they touch: removals from the back, then each inserted or out-of-order row at its place, then the rows whose stamp changed.
fn edits_between(old: &[Row], new: &[Row]) -> (Vec<Ed>, usize) {
    let in_new = |id: &str| new.iter().any(|(now, _)| now == id);
    let common: Vec<usize> = old.iter().filter_map(|(id, _)| new.iter().position(|(now, _)| now == id)).collect();
    let mut best: Vec<Vec<usize>> = Vec::new();
    for (at, value) in common.iter().enumerate() {
        let tail = (0..at).filter(|before| common[*before] < *value).map(|before| best[before].clone()).max_by_key(Vec::len).unwrap_or_default();
        best.push([tail, vec![*value]].concat());
    }
    let kept: Vec<usize> = best.into_iter().max_by_key(|chain| chain.len()).unwrap_or_default();
    let mut steps = Vec::new();
    let mut work: Vec<String> = old.iter().map(|(id, _)| id.clone()).collect();
    let leaving: Vec<usize> = (0..work.len()).rev().filter(|index| !in_new(&work[*index])).collect();
    for index in leaving {
        steps.push(Ed::Remove { index: index as u32, id: work.remove(index) });
    }
    let mut touched = steps.len();
    for (position, (id, _)) in new.iter().enumerate().filter(|(position, (id, _))| !old.iter().any(|(held, _)| held == id) || !kept.contains(position)) {
        let from = work.iter().position(|held| held == id);
        if let Some(from) = from {
            work.remove(from);
        }
        let to = if position == 0 { 0 } else { work.iter().position(|held| *held == new[position - 1].0).map_or(0, |before| before + 1) };
        work.insert(to, id.clone());
        touched += 1;
        match from {
            Some(from) if from != to => steps.push(Ed::Move { from: from as u32, to: to as u32 }),
            Some(_) => {}
            None => steps.push(Ed::Insert { index: to as u32, id: id.clone() }),
        }
    }
    for (position, (id, stamp)) in new.iter().enumerate() {
        if old.iter().any(|(held, before)| held == id && before != stamp) {
            steps.push(Ed::Rewrite { index: position as u32, id: id.clone() });
            if kept.contains(&position) {
                touched += 1;
            }
        }
    }
    (steps, touched)
}
