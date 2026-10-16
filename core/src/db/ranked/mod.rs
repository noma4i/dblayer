mod order_key;
mod tree;

use std::sync::Arc;

use crate::db::diff::Entry;
use crate::identity::touches::touch;

pub(crate) use order_key::OrderKey;
use tree::Tree;

type Placed = (Arc<OrderKey>, Arc<Entry>);

#[derive(Clone, Default)]
pub(crate) struct Ranked {
    ordered: Tree<Arc<OrderKey>, Arc<Entry>>,
    by_id: Tree<Arc<str>, Placed>,
}

impl Ranked {
    pub(crate) fn from_sorted(items: Vec<Placed>) -> Self {
        let mut by_id: Vec<(Arc<str>, Placed)> = items.iter().map(|(key, entry)| (Arc::from(entry.id.as_str()), (key.clone(), entry.clone()))).collect();
        by_id.sort_by(|left, right| left.0.cmp(&right.0));
        Self { ordered: Tree::from_sorted(&items), by_id: Tree::from_sorted(&by_id) }
    }

    pub(crate) fn len(&self) -> usize {
        self.ordered.len()
    }

    pub(crate) fn get(&self, id: &str) -> Option<(&Arc<OrderKey>, &Arc<Entry>)> {
        self.by_id.get(id).map(|(key, entry)| (key, entry))
    }

    pub(crate) fn position(&self, key: &OrderKey) -> usize {
        self.ordered.rank(key)
    }

    pub(crate) fn with(&self, key: Arc<OrderKey>, entry: Arc<Entry>) -> Self {
        let cleared = self.without(entry.id.as_str());
        Self { ordered: cleared.ordered.with(key.clone(), entry.clone()), by_id: cleared.by_id.with(Arc::from(entry.id.as_str()), (key, entry)) }
    }

    pub(crate) fn without(&self, id: &str) -> Self {
        match self.by_id.get(id) {
            Some((key, _)) => Self { ordered: self.ordered.without(key), by_id: self.by_id.without(id) },
            None => self.clone(),
        }
    }

    pub(crate) fn entries(&self) -> Vec<Arc<Entry>> {
        touch(self.len());
        self.ordered.values()
    }
}

#[cfg(test)]
mod order_key_tests;
#[cfg(test)]
mod tree_tests;
