use std::collections::{BTreeSet, HashMap};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MembershipChange {
    pub inserted: Vec<String>,
    pub moved: Vec<String>,
    pub removed: Vec<String>,
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
