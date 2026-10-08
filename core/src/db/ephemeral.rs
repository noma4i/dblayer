use std::collections::{BTreeMap, HashMap};
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
    windows: HashMap<String, Duration>,
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
        let row = self.rows.entry(key).or_default();
        for (field, value) in accepted {
            row.slots.insert(field.field.name.clone(), Slot { value, initial: field.initial(), due: field.reset_window().map(|window| now + window) });
        }
        if changed {
            self.epoch += 1;
            row.epoch = self.epoch;
        }
        Ok(changed.then_some(self.epoch))
    }

    pub(crate) fn forget(&mut self, model: &str, id: &str) {
        let key = (model.to_string(), id.to_string());
        self.rows.remove(&key);
        self.expiry.remove(&key);
    }

    pub(crate) fn clear(&mut self, models: Option<&[String]>) {
        let keep = |key: &RowKey| models.is_some_and(|models| !models.contains(&key.0));
        self.rows.retain(|key, _| keep(key));
        self.expiry.retain(|key, _| keep(key));
    }

    pub(crate) fn track(&mut self, model: &str, id: &str) -> bool {
        let Some(window) = self.windows.get(model).copied() else { return false };
        self.expiry.insert((model.to_string(), id.to_string()), self.clock.now() + window);
        true
    }

    pub(crate) fn take_due(&mut self) -> Due {
        let now = self.clock.now();
        let mut reset = Vec::new();
        let mut epoch = self.epoch;
        for (key, row) in self.rows.iter_mut() {
            let lapsed: Vec<String> = row.slots.iter().filter(|(_, slot)| slot.due.is_some_and(|due| due <= now)).map(|(name, _)| name.clone()).collect();
            if lapsed.is_empty() {
                continue;
            }
            let visible = lapsed.iter().any(|name| row.slots.get(name).is_some_and(|slot| slot.value != slot.initial));
            for name in &lapsed {
                row.slots.remove(name);
            }
            if visible {
                epoch += 1;
                row.epoch = epoch;
                reset.push(key.clone());
            }
        }
        self.epoch = epoch;
        let expired = self.lapsed(now, None);
        Due { reset, expired }
    }

    fn lapsed(&self, now: Duration, among: Option<&[RowKey]>) -> Vec<RowKey> {
        self.expiry.iter().filter(|(key, due)| **due <= now && among.is_none_or(|among| among.contains(key))).map(|(key, _)| key.clone()).collect()
    }

    pub(crate) fn claim_expired(&mut self, candidates: &[RowKey]) -> Vec<RowKey> {
        let claimed = self.lapsed(self.clock.now(), Some(candidates));
        for key in &claimed {
            self.expiry.remove(key);
        }
        claimed
    }

    fn next_due(&self) -> Option<Duration> {
        let slots = self.rows.values().flat_map(|row| row.slots.values()).filter_map(|slot| slot.due);
        slots.chain(self.expiry.values().copied()).min()
    }
}

impl Ephemerals {
    pub(crate) fn new(specs: &[ModelSpec]) -> Arc<Self> {
        let windows = specs.iter().filter_map(|spec| spec.expire_window().map(|window| (spec.key.as_str().to_string(), window))).collect();
        let state = State { clock: Arc::new(SystemClock), epoch: 0, closed: false, rows: HashMap::new(), expiry: HashMap::new(), windows };
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

    pub(crate) fn start(ephemerals: Arc<Ephemerals>, shared: Weak<Shared>) {
        std::thread::Builder::new()
            .name("dblayer-ephemeral".to_string())
            .spawn(move || {
                while ephemerals.wait_for_due() {
                    let Some(shared) = shared.upgrade() else { return };
                    let _ = Db { shared }.expire_ephemeral();
                }
            })
            .expect("ephemeral thread starts");
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
        self.read(|reader| {
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
        let mut destroyed = 0;
        let committed = self.shared.store.commit_with(generation, |reader| {
            let claimed = self.shared.ephemeral.lock().claim_expired(candidates);
            destroyed = claimed.len();
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
        self.shared.ephemeral.lock().claim_expired(candidates);
        committed.map_err(plan_fault)?;
        Ok(destroyed)
    }
}
