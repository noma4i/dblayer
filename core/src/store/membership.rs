use std::collections::{BTreeSet, HashMap, HashSet};

use crate::identity::RowId;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MembershipChange {
    pub inserted: Vec<String>,
    pub moved: Vec<String>,
    pub removed: Vec<String>,
}

impl MembershipChange {
    pub fn is_empty(&self) -> bool {
        self.inserted.is_empty() && self.moved.is_empty() && self.removed.is_empty()
    }

    pub fn touched_ids(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        self.inserted
            .iter()
            .chain(&self.moved)
            .chain(&self.removed)
            .filter(|id| seen.insert(id.as_str()))
            .cloned()
            .collect()
    }
}

pub struct MembershipPlan;

impl MembershipPlan {
    pub fn page(current: &[String], incoming: &[String]) -> MembershipChange {
        let mut positions: HashMap<&str, usize> = HashMap::new();
        for (index, id) in current.iter().enumerate() {
            positions.entry(id.as_str()).or_insert(index);
        }
        let mut change = MembershipChange::default();
        for (index, id) in incoming.iter().enumerate() {
            match positions.get(id.as_str()) {
                None => change.inserted.push(id.clone()),
                Some(position) if *position != index => change.moved.push(id.clone()),
                Some(_) => {}
            }
        }
        change
    }

    pub fn complete(current: &[String], incoming: &[String]) -> MembershipChange {
        let mut change = Self::page(current, incoming);
        let incoming_ids: BTreeSet<&str> = incoming.iter().map(String::as_str).collect();
        change.removed = current.iter().filter(|id| !incoming_ids.contains(id.as_str())).cloned().collect();
        change
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeRecord {
    pub row: RowId,
    pub order_key: String,
    pub has_row: bool,
}

impl ScopeRecord {
    pub fn new(row: RowId, order_key: &str) -> Self {
        Self { row, order_key: order_key.to_string(), has_row: true }
    }

    pub fn dangling(row: RowId, order_key: &str) -> Self {
        Self { row, order_key: order_key.to_string(), has_row: false }
    }

    pub fn visible(records: &[ScopeRecord]) -> Vec<RowId> {
        let mut present: Vec<&ScopeRecord> = records.iter().filter(|record| record.has_row).collect();
        present.sort_by(|left, right| left.order_key.cmp(&right.order_key).then_with(|| left.row.cmp(&right.row)));
        present.into_iter().map(|record| record.row.clone()).collect()
    }

    pub fn count(records: &[ScopeRecord]) -> usize {
        records.iter().filter(|record| record.has_row).count()
    }

    pub fn windowed(records: &[ScopeRecord], limit: Option<usize>) -> Vec<RowId> {
        let rows = Self::visible(records);
        match limit {
            Some(limit) => rows.into_iter().take(limit).collect(),
            None => rows,
        }
    }

    pub fn count_ignores_window(records: &[ScopeRecord]) -> bool {
        Self::count(records) == Self::visible(records).len()
    }
}
