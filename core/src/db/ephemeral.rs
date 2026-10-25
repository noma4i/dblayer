use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::Duration;

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
use crate::identity::{ModelKey, RowId, RowRef, ScopeKey};
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
    expiry: HashMap<RowKey, Duration>,
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
        if let Some(due) = self.expiry.remove(&from) {
            self.deadlines.remove(&(due, from, None));
            let due = *self.expiry.entry(to.clone()).or_insert(due);
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
        if let Some(due) = self.expiry.remove(&key) { self.deadlines.remove(&(due, key, None)); }
    }

    pub(crate) fn clear(&mut self, models: Option<&[String]>) {
        let keep = |key: &RowKey| models.is_some_and(|models| !models.contains(&key.0));
        self.rows.retain(|key, _| keep(key));
        self.expiry.retain(|key, _| keep(key));
        self.deadlines.retain(|(_, key, _)| keep(key));
    }

    pub(crate) fn track(&mut self, model: &str, id: &str) -> bool {
        let Some(window) = self.windows.get(model).copied() else { return false };
        let key = (model.to_string(), id.to_string());
        let due = self.clock.now() + window;
        if let Some(previous) = self.expiry.insert(key.clone(), due) { self.deadlines.remove(&(previous, key.clone(), None)); }
        self.deadlines.insert((due, key, None));
        true
    }

    pub(crate) fn take_due(&mut self) -> Due {
        let now = self.clock.now();
        let mut reset = BTreeSet::new();
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
            }
        }
        for key in &reset {
            self.epoch += 1;
            if let Some(row) = self.rows.get_mut(key) { row.epoch = self.epoch; }
        }
        Due { reset: reset.into_iter().collect(), expired }
    }

    pub(crate) fn claim_expired(&mut self, candidates: &[RowKey]) -> Vec<RowKey> {
        let now = self.clock.now();
        let mut claimed = Vec::new();
        for key in candidates {
            #[cfg(test)]
            self.due_visits.set(self.due_visits.get() + 1);
            if let Some(due) = self.expiry.get(key).copied().filter(|due| *due <= now) {
                self.expiry.remove(key);
                self.deadlines.remove(&(due, key.clone(), None));
                claimed.push(key.clone());
            }
        }
        claimed
    }

    fn next_due(&self) -> Option<Duration> {
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

    pub(crate) fn lock(&self) -> MutexGuard<'_, State> {
        locked(&self.state)
    }

    pub(crate) fn nudge(&self) {
        self.wake.notify_all();
    }

    pub(crate) fn close(&self) {
        self.lock().closed = true;
        self.wake.notify_all();
    }

    pub(crate) fn start(ephemerals: Arc<Ephemerals>, shared: Weak<Shared>) -> Result<(), DbError> {
        super::threads::spawn_named("dblayer-ephemeral", move || {
                while ephemerals.wait_for_due() {
                    let Some(shared) = shared.upgrade() else { return };
                    let _ = Db { shared }.expire_ephemeral();
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
                state = self.wake.wait(state).unwrap_or_else(|poisoned| poisoned.into_inner());
                continue;
            };
            let now = state.clock.now();
            if due <= now {
                return true;
            }
            state = self.wake.wait_timeout(state, due - now).unwrap_or_else(|poisoned| poisoned.into_inner()).0;
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
                    event.scopes.insert(ScopeRef::new(ModelKey::new(model), ScopeKey::from_raw(&key)));
                }
            }
            Ok(())
        })?;
        self.dispatch.push(event);
        Ok(())
    }
}

impl Db {
    pub fn set_ephemeral(&self, model: &str, id: &RowId, values: Map<String, Value>) -> Result<(), DbError> {
        let spec = self.shared.spec(model)?;
        let changed = self.shared.ephemeral.lock().write(spec, id.as_str(), &values)?;
        self.shared.ephemeral.nudge();
        match changed {
            Some(epoch) => self.shared.publish_overlay(&[(model.to_string(), id.to_string())], epoch),
            None => Ok(()),
        }
    }

    pub fn expire_ephemeral(&self) -> Result<usize, DbError> {
        let _gate = locked(&self.shared.expiry_gate);
        let (due, epoch) = {
            let mut state = self.shared.ephemeral.lock();
            let due = state.take_due();
            (due, state.epoch)
        };
        self.shared.publish_overlay(&due.reset, epoch)?;
        Ok(due.reset.len() + self.destroy_expired(&due.expired)?)
    }

    pub(crate) fn destroy_expired(&self, candidates: &[RowKey]) -> Result<usize, DbError> {
        if candidates.is_empty() {
            return Ok(0);
        }
        let generation = self.generation();
        let claimed = self.shared.ephemeral.lock().claim_expired(candidates);
        let destroyed = claimed.len();
        let committed = self.commit_following(generation, |reader| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Event, generation);
            let mut targets: Vec<RowRef> = Vec::new();
            for (model, id) in &claimed {
                let own = RowRef::new(ModelKey::new(model), RowId::new(id)?);
                for target in self.destroy_targets(reader, &own, WriteOrigin::Event)? {
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
