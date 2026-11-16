//! The ids the Store swapped (table `row_alias`) as the owners `Outbox` and `Transfers` hold them: a cache the facts of the Store write.

use std::collections::BTreeMap;

/// The swaps the Store committed, by temp id (a temp id names one row): the model and the real id.
#[derive(Debug, Default)]
pub(super) struct AliasMap(BTreeMap<String, (String, String)>);

impl AliasMap {
    pub(super) fn restored(aliases: BTreeMap<(String, String), String>) -> Self {
        Self(aliases.into_iter().map(|((model, temp), real)| (temp, (model, real))).collect())
    }

    pub(super) fn insert(&mut self, model: &str, temp: &str, real: &str) {
        self.0.insert(temp.to_string(), (model.to_string(), real.to_string()));
    }

    /// The real id of `row` in `model` when its `create` landed.
    pub(super) fn real(&self, model: &str, row: &str) -> Option<&String> {
        self.0.get(row).filter(|(swapped, _)| swapped == model).map(|(_, real)| real)
    }

    /// The real id of the temp id `temp` in any model.
    pub(super) fn real_of_temp(&self, temp: &str) -> Option<&String> {
        self.0.get(temp).map(|(_, real)| real)
    }

    pub(super) fn is_empty(&self) -> bool { self.0.is_empty() }

    /// The key `model:row:action` with the real id of its row when a swap gave one.
    pub(super) fn canonical_key(&self, key: &str) -> String {
        let Some((model, rest)) = key.split_once(':') else { return key.to_string() };
        let Some((row, action)) = rest.rsplit_once(':') else { return key.to_string() };
        self.real(model, row).map_or_else(|| key.to_string(), |real| format!("{model}:{real}:{action}"))
    }
}
