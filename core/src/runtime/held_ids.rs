use super::sync::{lock, Mutex};

/// The id of the unanswered request of a key: a repeat of the request under the key carries the held id until an answer settles it.
pub struct HeldIds<K> {
    held: Mutex<Vec<(K, u64)>>,
}

impl<K> Default for HeldIds<K> {
    fn default() -> Self { Self { held: Mutex::new(Vec::new()) } }
}

impl<K: PartialEq + Clone> HeldIds<K> {
    pub fn id_of(&self, key: &K, fresh: impl FnOnce() -> u64) -> u64 {
        let mut held = lock(&self.held);
        if let Some((_, id)) = held.iter().find(|(known, _)| known == key) { return *id; }
        let id = fresh();
        held.push((key.clone(), id));
        id
    }

    pub fn hold(&self, key: K, id: u64) {
        let mut held = lock(&self.held);
        held.retain(|(known, _)| *known != key);
        held.push((key, id));
    }

    pub fn settle(&self, id: u64) { lock(&self.held).retain(|(_, held)| *held != id); }
}
