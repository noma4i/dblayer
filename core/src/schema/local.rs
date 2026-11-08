use serde_json::{Map, Value};

use super::{FieldSpec, MergeRule, ModelSpec};

pub(crate) struct LocalSlot<'a> {
    pub field: &'a FieldSpec,
    pub key: Option<&'a str>,
}

impl FieldSpec {
    pub fn is_local(&self) -> bool {
        self.merge == Some(MergeRule::Local)
    }
}

impl ModelSpec {
    /// The server sends events for the model: it `listens`, declares `changed_by` or a relation declares `updates`.
    pub fn is_live(&self) -> bool {
        !self.listens.is_empty() || !self.changes.is_empty() || self.relations.iter().any(|relation| !relation.updates.is_empty())
    }

    pub(crate) fn has_local(&self) -> bool {
        self.fields.iter().any(|field| field.merge.as_ref().is_some_and(|rule| *rule == MergeRule::Local || rule.local_keys().next().is_some()))
    }

    pub(crate) fn local_slot<'a>(&'a self, path: &'a str) -> Option<LocalSlot<'a>> {
        let (name, key) = match path.split_once('.') {
            Some((name, key)) => (name, Some(key)),
            None => (path, None),
        };
        let field = self.field(name)?;
        let rule = field.merge.as_ref()?;
        let declared = match key {
            None => *rule == MergeRule::Local,
            Some(key) => rule.local_keys().any(|local| local == key),
        };
        declared.then_some(LocalSlot { field, key })
    }

    pub(crate) fn keep_local(&self, old: &Map<String, Value>, row: &mut Map<String, Value>) {
        for field in &self.fields {
            let Some(rule) = &field.merge else { continue };
            if *rule == MergeRule::Local {
                match old.get(&field.name) {
                    Some(value) => row.insert(field.name.clone(), value.clone()),
                    None => row.remove(&field.name),
                };
                continue;
            }
            let kept = old.get(&field.name).and_then(Value::as_object);
            for key in rule.local_keys() {
                let Some(Value::Object(target)) = row.get_mut(&field.name) else { break };
                match kept.and_then(|kept| kept.get(key)) {
                    Some(value) => target.insert(key.to_string(), value.clone()),
                    None => target.remove(key),
                };
            }
        }
    }
}

impl LocalSlot<'_> {
    pub(crate) fn write(&self, row: &mut Map<String, Value>, value: Value) {
        let Some(key) = self.key else {
            match value {
                Value::Null => row.remove(&self.field.name),
                value => row.insert(self.field.name.clone(), value),
            };
            return;
        };
        let parent = row.entry(self.field.name.clone()).or_insert_with(|| Value::Object(Map::new()));
        if !parent.is_object() {
            *parent = Value::Object(Map::new());
        }
        if let Value::Object(target) = parent {
            match value {
                Value::Null => target.remove(key),
                value => target.insert(key.to_string(), value),
            };
        }
    }
}
