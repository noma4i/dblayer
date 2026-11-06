use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
use crate::identity::{Generation, ModelKey, RowId, RowRef, ListKey};
use crate::commit::ScopeRef;
use crate::pacing::{Clock, SystemClock};
use crate::schema::ModelSpec;

use super::dispatch::Event;
use super::error::DbError;
use super::local_write::plan_fault;
use super::open::{locked, Db, Shared};

type RowKey = (String, String);

struct Slot {
    value: Value,
    initial: Value,
    due: Option<Duration>,
}

#[derive(Default)]
struct RowValues {
    epoch: u64,
    slots: BTreeMap<String, Slot>,
}

pub(crate) struct State {
    pub clock: Arc<dyn Clock>,
    pub epoch: u64,
    closed: bool,
    rows: HashMap<RowKey, RowValues>,
    expiry: HashMap<RowKey, (Duration, i64)>,
    deadlines: BTreeSet<(Duration, RowKey, Option<String>)>,
    windows: HashMap<String, Duration>,
    #[cfg(test)]
    pub(crate) due_visits: std::cell::Cell<usize>,
}

pub(crate) struct Due {
    pub reset: Vec<RowKey>,
    pub expired: Vec<RowKey>,
}

pub(crate) struct Ephemerals {
    state: Mutex<State>,
    wake: Condvar,
}

fn current(slot: Option<&Slot>, now: Duration) -> Option<&Value> {
    slot.filter(|slot| slot.due.is_none_or(|due| due > now)).map(|slot| &slot.value)
}

impl State {
    pub(crate) fn overlay(&self, spec: &ModelSpec, id: &str, data: &mut Map<String, Value>) -> u64 {
        let now = self.clock.now();
        let row = self.rows.get(&(spec.key.as_str().to_string(), id.to_string()));
        for field in &spec.ephemeral {
            let held = current(row.and_then(|row| row.slots.get(&field.field.name)), now);
            data.insert(field.field.name.clone(), held.cloned().unwrap_or_else(|| field.initial()));
        }
        row.map_or(0, |row| row.epoch)
    }

    fn write(&mut self, spec: &ModelSpec, id: &str, values: &Map<String, Value>) -> Result<Option<u64>, DbError> {
        let model = spec.key.as_str();
        let mut accepted = Vec::new();
        for (name, value) in values {
            let field = spec.ephemeral_field(name).ok_or_else(|| DbError::NotEphemeralField { model: model.to_string(), field: name.clone() })?;
            let decoded = if value.is_null() && field.field.nullable { Some(Value::Null) } else { field.field.decode(value) };
            let decoded = decoded.ok_or_else(|| DbError::InvalidEphemeralValue { model: model.to_string(), field: name.clone() })?;
            accepted.push((field, decoded));
        }
        let now = self.clock.now();
        let key = (model.to_string(), id.to_string());
        let previous: Vec<Value> = accepted
            .iter()
            .map(|(field, _)| current(self.rows.get(&key).and_then(|row| row.slots.get(&field.field.name)), now).cloned().unwrap_or_else(|| field.initial()))
            .collect();
        let changed = accepted.iter().zip(&previous).any(|((_, value), before)| value != before);
        let row = self.rows.entry(key.clone()).or_default();
        for (field, value) in accepted {
            let name = field.field.name.clone();
            let due = field.reset_window().map(|window| now + window);
            if let Some(previous) = row.slots.insert(name.clone(), Slot { value, initial: field.initial(), due }).and_then(|slot| slot.due) {
                self.deadlines.remove(&(previous, key.clone(), Some(name.clone())));
            }
            if let Some(due) = due { self.deadlines.insert((due, key.clone(), Some(name))); }
        }
        if changed {
            self.epoch += 1;
            row.epoch = self.epoch;
        }
        Ok(changed.then_some(self.epoch))
    }

    pub(crate) fn rename(&mut self, model: &str, from: &str, to: &str) {
        let (from, to) = ((model.to_string(), from.to_string()), (model.to_string(), to.to_string()));
        if let Some(values) = self.rows.remove(&from) {
            for (name, slot) in &values.slots {
                if let Some(due) = slot.due {
                    self.deadlines.remove(&(due, from.clone(), Some(name.clone())));
                    if !self.rows.contains_key(&to) { self.deadlines.insert((due, to.clone(), Some(name.clone()))); }
                }
            }
            self.rows.entry(to.clone()).or_insert(values);
        }
        if let Some(armed) = self.expiry.remove(&from) {
            self.deadlines.remove(&(armed.0, from, None));
            let due = self.expiry.entry(to.clone()).or_insert(armed).0;
            self.deadlines.insert((due, to, None));
        }
    }

    pub(crate) fn forget(&mut self, model: &str, id: &str) {
        let key = (model.to_string(), id.to_string());
        if let Some(row) = self.rows.remove(&key) {
            for (name, slot) in row.slots {
                if let Some(due) = slot.due { self.deadlines.remove(&(due, key.clone(), Some(name))); }
            }
        }
        if let Some((due, _)) = self.expiry.remove(&key) { self.deadlines.remove(&(due, key, None)); }
    }

    pub(crate) fn clear(&mut self, models: Option<&[String]>) {
        let keep = |key: &RowKey| models.is_some_and(|models| !models.contains(&key.0));
        self.rows.retain(|key, _| keep(key));
        self.expiry.retain(|key, _| keep(key));
        self.deadlines.retain(|(_, key, _)| keep(key));
    }

    pub(crate) fn track(&mut self, model: &str, id: &str, revision: i64) -> bool {
        let Some(window) = self.windows.get(model).copied() else { return false };
        let key = (model.to_string(), id.to_string());
        let due = self.clock.now() + window;
        if let Some((previous, _)) = self.expiry.insert(key.clone(), (due, revision)) { self.deadlines.remove(&(previous, key.clone(), None)); }
        self.deadlines.insert((due, key, None));
        true
    }

    pub(crate) fn take_due(&mut self) -> Due {
        let now = self.clock.now();
        let mut reset = BTreeSet::new();
        let mut lapsed = BTreeSet::new();
        let mut expired = Vec::new();
        let due: Vec<_> = self.deadlines.iter().take_while(|(due, _, _)| *due <= now).cloned().collect();
        for entry @ (_, ref key, ref name) in &due {
            #[cfg(test)]
            self.due_visits.set(self.due_visits.get() + 1);
            let Some(name) = name else {
                expired.push(key.clone());
                continue;
            };
            self.deadlines.remove(entry);
            if let Some(slot) = self.rows.get_mut(key).and_then(|row| row.slots.remove(name)) {
                if slot.value != slot.initial { reset.insert(key.clone()); }
                lapsed.insert(key.clone());
            }
        }
        reset.extend(lapsed.iter().filter(|key| self.rows.get(*key).is_some_and(|row| row.slots.is_empty() && row.epoch != 0)).cloned());
        for key in &reset {
            self.epoch += 1;
            if let Some(row) = self.rows.get_mut(key) { row.epoch = self.epoch; }
        }
        for key in &lapsed {
            if self.rows.get(key).is_some_and(|row| row.slots.is_empty()) { self.rows.remove(key); }
        }
        Due { reset: reset.into_iter().collect(), expired }
    }

    pub(crate) fn claim_expired(&mut self, candidates: &[RowKey]) -> Vec<(RowKey, i64)> {
        let now = self.clock.now();
        let mut claimed = Vec::new();
        for key in candidates {
            #[cfg(test)]
            self.due_visits.set(self.due_visits.get() + 1);
            if let Some((due, revision)) = self.expiry.get(key).copied().filter(|(due, _)| *due <= now) {
                self.expiry.remove(key);
                self.deadlines.remove(&(due, key.clone(), None));
                claimed.push((key.clone(), revision));
            }
        }
        claimed
    }

    #[cfg(test)]
    pub(crate) fn held_rows(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn next_due(&self) -> Option<Duration> {
        self.deadlines.first().map(|(due, _, _)| *due)
    }
}

impl Ephemerals {
    pub(crate) fn new(specs: &[ModelSpec]) -> Arc<Self> {
        let windows = specs.iter().filter_map(|spec| spec.expire_window().map(|window| (spec.key.as_str().to_string(), window))).collect();
        let state = State { clock: Arc::new(SystemClock), epoch: 0, closed: false, rows: HashMap::new(), expiry: HashMap::new(), deadlines: BTreeSet::new(), windows,
            #[cfg(test)]
            due_visits: std::cell::Cell::new(0),
        };
        Arc::new(Self { state: Mutex::new(state), wake: Condvar::new() })
    }

    pub(crate) fn lock(&self) -> super::lock::Guard<'_, State> {
        locked(&self.state)
    }

    pub(crate) fn nudge(&self) {
        self.wake.notify_all();
    }

    pub(crate) fn wake_for_clock(&self) {
        drop(self.lock());
        self.wake.notify_all();
    }

    pub(crate) fn close(&self) {
        self.lock().closed = true;
        self.wake.notify_all();
    }

    pub(crate) fn start(threads: &Arc<super::threads::LiveThreads>, ephemerals: Arc<Ephemerals>, shared: Weak<Shared>) -> Result<(), DbError> {
        threads.start(crate::task::Carry::Detached, "dblayer-ephemeral", move || {
                while ephemerals.wait_for_due() {
                    let Some(shared) = shared.upgrade() else { return };
                    let db = Db { shared };
                    if let Ok(admitted) = db.admit() { let _ = db.expire_ephemeral(&admitted); }
                }
            })
    }

    fn wait_for_due(&self) -> bool {
        let mut state = self.lock();
        loop {
            if state.closed {
                return false;
            }
            let Some(due) = state.next_due() else {
                state = super::lock::wait(&self.wake, state);
                continue;
            };
            let now = state.clock.now();
            if due <= now {
                return true;
            }
            state = super::lock::wait_timeout(&self.wake, state, due - now).0;
        }
    }
}

impl Shared {
    pub(crate) fn publish_overlay(&self, touched: &[RowKey], epoch: u64) -> Result<(), DbError> {
        if touched.is_empty() {
            return Ok(());
        }
        {
            let mut cache = locked(&self.cache);
            for (model, id) in touched {
                cache.evict(model, id);
            }
        }
        let mut event = Event { epoch, ..Event::default() };
        self.read(&[], |reader| {
            event.revision = reader.revision_seq()?;
            for (model, id) in touched {
                event.models.insert(ModelKey::new(model));
                event.rows.insert(RowRef::new(ModelKey::new(model), RowId::new(id)?));
                for key in reader.scopes_of(model, id)? {
                    event.scopes.insert(ScopeRef::new(ModelKey::new(model), ListKey::from_raw(&key)));
                }
            }
            Ok(())
        })?;
        self.dispatch.push(event);
        Ok(())
    }
}

impl Db {
    /// The write of the owner LocalWrites: the epoch was checked by the owner.
    /// An update that changes nothing still renews the expiry of a memory row: the write counts as the last write of the row.
    pub(crate) fn renew_ephemeral(&self, model: &str, id: &RowId) {
        self.shared.ephemeral.lock().track(model, id.as_str(), i64::MAX);
        self.shared.ephemeral.nudge();
    }

    pub(crate) fn write_ephemeral(&self, model: &str, id: &RowId, values: &Map<String, Value>) -> Result<(), DbError> {
        let spec = self.shared.spec(model)?;
        let changed = self.shared.ephemeral.lock().write(spec, id.as_str(), values)?;
        self.shared.ephemeral.nudge();
        match changed {
            Some(epoch) => self.shared.publish_overlay(&[(model.to_string(), id.to_string())], epoch),
            None => Ok(()),
        }
    }

    pub fn set_ephemeral(&self, admitted: &crate::ffi::admission::Admitted, model: &str, id: &RowId, values: Map<String, Value>) -> Result<(), DbError> {
        let spec = self.shared.spec(model)?;
        let changed = {
            let life = &self.shared.store.lifecycle;
            let _line = life.commit_line();
            if !Arc::ptr_eq(life, admitted.life()) || !admitted.is_current() {
                return Err(DbError::NoSession { detail: "the session of this write ended".into() });
            }
            self.shared.ephemeral.lock().write(spec, id.as_str(), &values)?
        };
        self.shared.ephemeral.nudge();
        match changed {
            Some(epoch) => self.shared.publish_overlay(&[(model.to_string(), id.to_string())], epoch),
            None => Ok(()),
        }
    }

    pub fn expire_ephemeral(&self, admitted: &crate::ffi::admission::Admitted) -> Result<usize, DbError> {
        let _gate = self.shared.expiry_gate.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (due, epoch) = {
            let life = &self.shared.store.lifecycle;
            let _line = life.commit_line();
            if !Arc::ptr_eq(life, admitted.life()) || !admitted.is_current() { return Err(DbError::NoSession { detail: "the session of this expiry ended".into() }); }
            let mut state = self.shared.ephemeral.lock();
            let due = state.take_due();
            (due, state.epoch)
        };
        self.shared.publish_overlay(&due.reset, epoch)?;
        Ok(due.reset.len() + self.destroy_expired(admitted, &due.expired)?)
    }

    pub(crate) fn destroy_expired(&self, admitted: &crate::ffi::admission::Admitted, candidates: &[RowKey]) -> Result<usize, DbError> {
        if candidates.is_empty() {
            return Ok(0);
        }
        let generation = self.generation();
        let claimed = {
            let life = &self.shared.store.lifecycle;
            let _line = life.commit_line();
            if !Arc::ptr_eq(life, admitted.life()) || !admitted.is_current() { return Err(DbError::NoSession { detail: "the session of this expiry ended".into() }); }
            self.shared.ephemeral.lock().claim_expired(candidates)
        };
        self.destroy_claimed(admitted, generation, &claimed)
    }

    pub(crate) fn destroy_claimed(&self, admitted: &crate::ffi::admission::Admitted, generation: Generation, claimed: &[(RowKey, i64)]) -> Result<usize, DbError> {
        let mut destroyed = 0;
        let committed = self.commit_following(&admitted.authority(), generation, |reader| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Event, generation);
            let mut targets: Vec<RowRef> = Vec::new();
            let mut expired: Vec<&RowKey> = Vec::new();
            for (key, armed) in claimed {
                if reader.stored_row(&key.0, &key.1)?.is_some_and(|row| row.revision <= *armed) {
                    expired.push(key);
                }
            }
            destroyed = expired.len();
            for (model, id) in expired {
                let own = RowRef::new(ModelKey::new(model), RowId::new(id)?);
                for target in self.destroy_targets(reader, &own)? {
                    if !targets.contains(&target) {
                        targets.push(target);
                    }
                }
            }
            envelope.entity_ops = targets.into_iter().map(|target| EntityOp::Destroy { model: target.model, id: target.id, reason: DestroyReason::Expired }).collect();
            Ok::<_, DbError>(Some(envelope))
        });
        committed.map_err(plan_fault)?;
        Ok(destroyed)
    }
}
