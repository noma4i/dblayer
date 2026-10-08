use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainVerdict {
    Intact,
    LostMembership,
    LostRows,
    Empty,
}

impl ChainVerdict {
    pub fn of(chain_ids: &[String], members: &BTreeSet<String>, live_rows: &BTreeSet<String>) -> Self {
        if chain_ids.is_empty() {
            return ChainVerdict::Empty;
        }
        if chain_ids.iter().any(|id| !live_rows.contains(id)) {
            return ChainVerdict::LostRows;
        }
        if chain_ids.iter().any(|id| !members.contains(id)) {
            return ChainVerdict::LostMembership;
        }
        ChainVerdict::Intact
    }

    pub fn is_stale(self) -> bool {
        self != ChainVerdict::Intact
    }
}

pub fn surviving_ids(chain_ids: &[String], live_rows: &BTreeSet<String>) -> Vec<String> {
    chain_ids.iter().filter(|id| live_rows.contains(*id)).cloned().collect()
}

pub fn chain_shrinks(chain_ids: &[String], live_rows: &BTreeSet<String>) -> bool {
    let survivors = surviving_ids(chain_ids, live_rows).len();
    survivors < chain_ids.len() && survivors > 0
}
