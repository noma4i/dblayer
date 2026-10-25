use serde_json::json;

use crate::commit::ScopeRef;
use crate::db::fixtures::*;
use crate::db::ReplaceRequest;
use crate::identity::{ModelKey, ListKey};
use crate::schema::Coverage;

struct Seed(u64);

impl Seed {
    fn below(&mut self, bound: usize) -> usize {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) as usize % bound
    }

    fn some_of(&mut self, pool: usize) -> Vec<String> {
        let mut left: Vec<usize> = (0..pool).collect();
        let size = self.below(pool);
        (0..size).map(|_| format!("m{:02}", left.remove(self.below(left.len())))).collect()
    }
}

fn rows(ids: &[String]) -> Vec<serde_json::Map<String, serde_json::Value>> {
    ids.iter().map(|id| obj(json!({"id": id, "userId": "u1", "text": id}))).collect()
}

fn composed(current: &[String], incoming: &[String], coverage: Coverage) -> Vec<String> {
    let mut result = incoming.to_vec();
    if coverage == Coverage::Page {
        let anchor = incoming.last().and_then(|last| current.iter().position(|id| id == last));
        result.extend(anchor.into_iter().flat_map(|position| current[position + 1..].iter().filter(|id| !incoming.contains(id)).cloned()));
    }
    result
}

#[test]
fn inv_merge_07_the_list_after_a_merge_follows_the_complete_and_page_rules_for_random_lists() {
    let scope = ScopeRef::new(ModelKey::new("moments"), ListKey::from_raw("feed"));
    let mut seed = Seed(7);
    for case in 0..100 {
        for coverage in [Coverage::Complete, Coverage::Page] {
            let db = booted();
            let (current, incoming) = (seed.some_of(12), seed.some_of(12));
            db.replace(&ReplaceRequest::new(scope.clone(), Coverage::Complete), rows(&current)).unwrap();
            db.replace(&ReplaceRequest::new(scope.clone(), coverage), rows(&incoming)).unwrap();
            let members: Vec<String> = db.shared.store.scope_members("moments", "feed").unwrap().into_iter().map(|(id, _)| id).collect();
            assert_eq!(members, composed(&current, &incoming, coverage), "case {case} {coverage:?} current {current:?} incoming {incoming:?}");
        }
    }
}
