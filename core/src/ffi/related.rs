use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use rusqlite::types::Value;
use crate::db::{locked, Change, Diff, Entry, OrderKey, Ranked, Related, Row, Stamp};
use crate::identity::RowId;
use crate::pacing::Counters;
use crate::schema::AssociationKind;
use super::live::LiveChange;
use super::wire::WireModel;

pub(super) struct RelatedProjection {
    name: String,
    kind: AssociationKind,
    counters: Arc<Mutex<Counters>>,
    keys: HashMap<RowId, Arc<OrderKey>>,
    found: HashMap<RowId, Vec<Row>>,
    rows: HashMap<RowId, Row>,
    owners: HashMap<RowId, BTreeSet<RowId>>,
    shown: Ranked,
    started: bool,
    positional: Option<Vec<Row>>,
}

impl RelatedProjection {
    pub(super) fn new(name: String, kind: AssociationKind, counters: Arc<Mutex<Counters>>) -> Self {
        Self { name, kind, counters, keys: HashMap::new(), found: HashMap::new(), rows: HashMap::new(), owners: HashMap::new(), shown: Ranked::default(), started: false, positional: None }
    }

    pub(super) fn apply(&mut self, change: &Change, wire: &WireModel) -> Option<LiveChange> {
        let first = !std::mem::replace(&mut self.started, true);
        if self.positional.is_none() && self.keys.is_empty() && change.rows.iter().any(|row| !change.keys.contains_key(&row.id)) {
            self.positional = Some(Vec::new());
        }
        let mut affected: HashSet<RowId> = HashSet::new();
        if change.full {
            self.drop_all(&mut affected);
        } else {
            for gone in change.diff.removed.iter().chain(change.diff.replaced.iter().map(|(from, _)| from)) {
                self.drop_parent(gone, &mut affected);
            }
        }
        let per_parent = change.related.get(&self.name);
        for parent in &change.rows {
            let targets = per_parent.and_then(|found| found.get(&parent.id)).cloned().unwrap_or_default();
            self.replace_parent(&parent.id, targets, &mut affected);
        }
        let structural = change.full || !(change.diff.inserted.is_empty() && change.diff.removed.is_empty() && change.diff.moved.is_empty() && change.diff.replaced.is_empty());
        if let Some(parents) = self.positional.as_mut() {
            change.apply_to(parents);
            if structural {
                self.keys = parents.iter().enumerate().map(|(index, parent)| (parent.id.clone(), Arc::new(OrderKey::new(vec![Value::Integer(index as i64)], &[false], parent.id.to_string())))).collect();
                affected.extend(self.found.values().flatten().map(|target| target.id.clone()));
            }
        } else {
            for (parent, key) in &change.keys {
                if self.keys.get(parent).is_some_and(|known| known == key) {
                    continue;
                }
                self.keys.insert(parent.clone(), key.clone());
                affected.extend(self.found.get(parent).into_iter().flatten().map(|target| target.id.clone()));
            }
        }
        self.emit(&affected, first, change.revision, wire)
    }

    fn emit(&mut self, affected: &HashSet<RowId>, first: bool, revision: i64, wire: &WireModel) -> Option<LiveChange> {
        let before = self.shown.clone();
        let mut sendable: HashMap<RowId, Row> = HashMap::new();
        for target in affected {
            locked(&self.counters).related_visited += 1;
            match self.first_owner(target) {
                Some((key, row)) => {
                    self.shown = self.shown.with(Arc::new(key), Arc::new(Entry { id: target.clone(), stamp: Stamp::of(&row, Vec::new()) }));
                    sendable.insert(target.clone(), row);
                }
                None => self.shown = self.shown.without(target.as_str()),
            }
        }
        let diff = if first {
            Diff::inserting(&self.shown.entries(), &[])
        } else {
            let names: HashSet<String> = affected.iter().map(ToString::to_string).collect();
            Diff::stepping(&before, &self.shown, &names, &[])
        };
        if !first && diff.is_empty() {
            return None;
        }
        let mut sent: Vec<&RowId> = if first { sendable.keys().collect() } else { diff.inserted.iter().chain(&diff.updated).collect() };
        sent.sort_by_key(|id| diff.at.get(*id).copied().unwrap_or(usize::MAX));
        let sent: Vec<Row> = sent.into_iter().filter_map(|id| sendable.get(id).cloned()).collect();
        let sent = Change { revision, rows: sent, related: Related::new(), diff, full: first, size: self.shown.len(), window: None, keys: HashMap::new() };
        Some(LiveChange::rows(&sent, wire))
    }

    fn replace_parent(&mut self, parent: &RowId, targets: Vec<Row>, affected: &mut HashSet<RowId>) {
        let old = self.found.remove(parent).unwrap_or_default();
        locked(&self.counters).related_visited += targets.len();
        let by_id: HashMap<&RowId, (usize, &Row)> = old.iter().enumerate().map(|(index, row)| (&row.id, (index, row))).collect();
        let positioned = matches!(self.kind, AssociationKind::References);
        for (index, row) in targets.iter().enumerate() {
            self.owners.entry(row.id.clone()).or_default().insert(parent.clone());
            match by_id.get(&row.id) {
                Some((was_at, was)) if Arc::ptr_eq(was, row) && (!positioned || *was_at == index) => {}
                _ => {
                    self.rows.insert(row.id.clone(), row.clone());
                    affected.insert(row.id.clone());
                }
            }
        }
        let kept: HashSet<&RowId> = targets.iter().map(|row| &row.id).collect();
        for row in old.iter().filter(|row| !kept.contains(&row.id)) {
            self.release(&row.id, parent);
            affected.insert(row.id.clone());
        }
        self.found.insert(parent.clone(), targets);
    }

    fn release(&mut self, target: &RowId, parent: &RowId) {
        let Some(owners) = self.owners.get_mut(target) else { return };
        owners.remove(parent);
        if owners.is_empty() {
            self.owners.remove(target);
            self.rows.remove(target);
        }
    }

    fn first_owner(&self, target: &RowId) -> Option<(OrderKey, Row)> {
        let row = self.rows.get(target)?;
        self.owners
            .get(target)?
            .iter()
            .filter_map(|parent| {
                let key = self.keys.get(parent)?;
                let term = match self.kind {
                    AssociationKind::HasOne | AssociationKind::HasMany => Value::Text(target.to_string()),
                    AssociationKind::BelongsTo => Value::Integer(0),
                    AssociationKind::References => Value::Integer(self.found.get(parent)?.iter().position(|found| found.id == *target)? as i64),
                };
                Some(key.extended(term, target.as_str()))
            })
            .min()
            .map(|key| (key, row.clone()))
    }

    fn drop_parent(&mut self, parent: &RowId, affected: &mut HashSet<RowId>) {
        for target in self.found.remove(parent).unwrap_or_default() {
            self.release(&target.id, parent);
            affected.insert(target.id.clone());
        }
        self.keys.remove(parent);
    }

    fn drop_all(&mut self, affected: &mut HashSet<RowId>) {
        affected.extend(self.owners.keys().cloned());
        self.found.clear();
        self.owners.clear();
        self.rows.clear();
        self.keys.clear();
        if let Some(parents) = self.positional.as_mut() {
            parents.clear();
        }
    }
}
