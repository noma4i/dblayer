use rusqlite::types::Value as SqlValue;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::store::StoreError;
use super::prior::{Prior, PriorRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectOwner {
    New { transition: usize },
    Existing { sequence: i64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectKind {
    Create,
    Edit,
    Destroy,
    Join,
    Leave,
}

impl EffectKind {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Edit => "edit",
            Self::Destroy => "destroy",
            Self::Join => "join",
            Self::Leave => "leave",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperationEffect {
    pub owner: EffectOwner,
    pub model: String,
    pub row_id: String,
    pub scope_key: Option<String>,
    pub kind: EffectKind,
    pub revision: i64,
    pub before: Value,
}

pub(crate) fn write_operation_effects(connection: &Connection, sequence: i64, effects: &[OperationEffect]) -> Result<(), StoreError> {
    remove_operation_effects(connection, sequence)?;
    for (position, effect) in effects.iter().enumerate() {
        let position = position as i64;
        connection.execute(
            "INSERT INTO op_effect (seq, position, model, row_id, scope_key, kind, revision) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![sequence, position, effect.model, effect.row_id, effect.scope_key, effect.kind.as_sql(), effect.revision],
        )?;
        crate::nodes::write_tree(connection, "op_effect_value", &["seq", "position"], &[SqlValue::Integer(sequence), SqlValue::Integer(position)], &effect.before)?;
    }
    Ok(())
}

pub(crate) fn remove_operation_effects(connection: &Connection, sequence: i64) -> Result<(), StoreError> {
    connection.execute("DELETE FROM op_effect_value WHERE seq = ?1", [sequence])?;
    connection.execute("DELETE FROM op_effect WHERE seq = ?1", [sequence])?;
    Ok(())
}

pub(crate) fn open_scope_effects(connection: &Connection, model: &str, scope_key: &str) -> Result<Vec<(String, bool)>, StoreError> {
    let sql = format!("SELECT e.row_id, e.kind FROM op_effect AS e JOIN op ON op.seq = e.seq LEFT JOIN scope_revision r ON r.model = e.model AND r.scope_key = e.scope_key AND r.row_id = e.row_id WHERE e.model = ?1 AND e.scope_key = ?2 AND e.kind IN ('join', 'leave') AND e.seq >= COALESCE(r.operation_seq, 0) AND op.status IN {} ORDER BY e.seq, e.position", crate::operation::OPEN_STATUSES);
    let mut statement = connection.prepare_cached(&sql)?;
    let rows = statement.query_map(params![model, scope_key], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)? == "join")))?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

pub(crate) fn write_prior(connection: &Connection, sequence: i64, revision: i64, value: Option<&Value>) -> Result<(), StoreError> {
    let Some(value) = value else { return remove_operation_effects(connection, sequence) };
    let prior: Prior = serde_json::from_value(value.clone())?;
    let effects = prior.rows.into_iter().map(|row| {
        let (model, row_id, scope_key, kind) = match &row {
            PriorRow::Created { model, id } => (model, id, None, EffectKind::Create),
            PriorRow::Edited { model, id, .. } => (model, id, None, EffectKind::Edit),
            PriorRow::Destroyed { model, id, .. } => (model, id, None, EffectKind::Destroy),
            PriorRow::Join { model, id, scope_key, .. } => (model, id, Some(scope_key.clone()), EffectKind::Join),
            PriorRow::Leave { model, id, scope_key, .. } => (model, id, Some(scope_key.clone()), EffectKind::Leave),
        };
        let held_revision: Option<i64> = connection.query_row(
            "SELECT revision FROM op_effect WHERE seq = ?1 AND model = ?2 AND row_id = ?3 AND scope_key IS ?4 AND kind = ?5 LIMIT 1",
            params![sequence, model, row_id, scope_key, kind.as_sql()], |row| row.get(0),
        ).optional()?;
        let owned = match &row {
            PriorRow::Join { owned, .. } | PriorRow::Leave { owned, .. } => *owned,
            _ => held_revision.unwrap_or(revision),
        };
        Ok(OperationEffect { owner: EffectOwner::Existing { sequence }, model: model.clone(), row_id: row_id.clone(), scope_key, kind, revision: owned, before: serde_json::to_value(&row)? })
    }).collect::<Result<Vec<_>, StoreError>>()?;
    write_operation_effects(connection, sequence, &effects)?;
    for effect in &effects {
        if let Some(scope_key) = &effect.scope_key {
            if effect.revision == revision {
                connection.execute("INSERT INTO scope_revision (model, scope_key, row_id, revision, operation_seq) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT (model, scope_key, row_id) DO UPDATE SET revision = excluded.revision, operation_seq = excluded.operation_seq", params![effect.model, scope_key, effect.row_id, revision, sequence])?;
            }
        }
    }
    Ok(())
}

pub(crate) fn read_prior(connection: &Connection, sequence: i64) -> Result<Option<Value>, StoreError> {
    let rows = crate::nodes::read_trees(connection, "op_effect_value", &["seq", "position"], "seq = ?1", &[SqlValue::Integer(sequence)])?;
    if rows.is_empty() { return Ok(None) }
    Ok(Some(serde_json::json!({ "rows": rows.into_iter().map(|(_, value)| value).collect::<Vec<_>>() })))
}
