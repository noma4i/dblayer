use std::collections::HashSet;

use super::operation_status::{is_open, Operation};
use crate::identity::{ModelKey, RowId};

#[derive(Debug, Clone)]
pub struct OutboxQueue {
    operations: Vec<Operation>,
}

impl OutboxQueue {
    pub fn new(operations: Vec<Operation>) -> Self {
        Self { operations: operations.into_iter().filter(|operation| is_open(operation.status)).collect() }
    }

    fn of_model<'a, 'm>(&'a self, model: &'m str) -> impl Iterator<Item = &'a Operation> + use<'a, 'm> {
        self.operations
            .iter()
            .filter(move |operation| Operation::split_action_key(&operation.action_key).is_some_and(|(owner, _)| owner == model))
    }

    pub fn open_row_ids(&self, model: &ModelKey) -> HashSet<RowId> {
        let mut ids = HashSet::new();
        for operation in self.of_model(model.as_str()) {
            ids.extend(operation.row_ids.iter().cloned());
            ids.extend(operation.temp_id.iter().cloned());
        }
        ids
    }

}
