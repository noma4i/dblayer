use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::identity::RowId;
use crate::schema::EffectSource;
use crate::store::Reader;
use crate::value::read_id;

use crate::store::DbError;
use crate::store::Store;

type Row = Map<String, Value>;

pub(crate) struct Edits {
    rows: BTreeMap<(String, String), (Row, Row)>,
}

impl Edits {
    pub(crate) fn of_owner(model: &str, id: &RowId, current: Row, merged: Row) -> Self {
        Self { rows: BTreeMap::from([((model.to_string(), id.to_string()), (current, merged))]) }
    }

    pub(crate) fn into_rows(self) -> impl Iterator<Item = ((String, String), (Row, Row))> {
        self.rows.into_iter()
    }

}

impl Store {
    fn effect_row(&self, reader: &Reader<'_>, args: &Map<String, Value>, model: &str, row: Option<&str>) -> Result<Option<RowId>, DbError> {
        if let Some(name) = row {
            return Ok(args.get(name).and_then(read_id));
        }
        let Some(relation) = self.spec(model)?.singleton_relation() else { return Ok(None) };
        let key = self.spec(model)?.selection_key(&relation.name, &Map::new())?;
        reader.scope_members(model, key.as_str())?.into_iter().next().map(|(id, _)| Ok(RowId::new(&id)?)).transpose()
    }

    pub(crate) fn fill_local_sources(&self, model: &str, values: &mut Map<String, Value>) -> Result<(), DbError> {
        let spec = self.spec(model)?;
        let Some(create) = spec.actions.iter().find(|declared| declared.name == "create") else { return Ok(()) };
        if create.local_from.is_empty() { return Ok(()); }
        let sources: std::collections::BTreeSet<&str> = create.local_from.values().filter_map(|source| match source {
            EffectSource::Row { model, row: None, .. } => Some(model.as_str()),
            _ => None,
        }).collect();
        self.read_models(&sources.into_iter().collect::<Vec<_>>(), |reader| {
            let mut rows = std::collections::BTreeMap::new();
            for (field, source) in &create.local_from {
                if values.get(field).is_some_and(|value| !value.is_null()) { continue; }
                let EffectSource::Row { model: from, row: None, field: from_field } = source else { continue };
                if !rows.contains_key(from) {
                    let row = match self.effect_row(reader, &Map::new(), from, None)? {
                        Some(id) => reader.point(from, id.as_str())?,
                        None => None,
                    };
                    rows.insert(from.clone(), row);
                }
                let found = rows.get(from).and_then(Option::as_ref).and_then(|row| row.get(from_field)).filter(|value| !value.is_null()).cloned();
                if let Some(value) = found { values.insert(field.clone(), value); }
            }
            Ok(())
        })
    }

    /// The models besides the receiver that the copy of `declared` reads.
    pub(crate) fn copy_source_models(copy: &crate::schema::ReceiverCopy) -> Vec<&str> {
        copy.set.values().filter_map(|value| match value {
            crate::schema::CopyValue::Source(EffectSource::Row { model, .. }) => Some(model.as_str()),
            _ => None,
        }).collect()
    }

    /// The fields `set:` of a copy of the receiver: a literal, a field of the receiver, or a field of the row of another model.
    pub(crate) fn apply_copy_set(&self, reader: &Reader<'_>, copy: &crate::schema::ReceiverCopy, receiver: &Row, args: &Map<String, Value>, values: &mut Row) -> Result<(), DbError> {
        for (field, source) in &copy.set {
            let value = match source {
                crate::schema::CopyValue::Literal(value) => value.clone(),
                crate::schema::CopyValue::Source(EffectSource::Owner(from)) => receiver.get(from).cloned().unwrap_or(Value::Null),
                crate::schema::CopyValue::Source(EffectSource::Row { model, row, field: from }) => {
                    let id = self.effect_row(reader, args, model, row.as_deref())?.ok_or_else(|| DbError::NotFound { model: model.clone(), id: format!("the row of {model} that {field} copies") })?;
                    if from == "id" {
                        Value::String(id.to_string())
                    } else {
                        reader.point(model, id.as_str())?.and_then(|found| found.get(from).cloned()).unwrap_or(Value::Null)
                    }
                }
            };
            values.insert(field.clone(), value);
        }
        Ok(())
    }
}
