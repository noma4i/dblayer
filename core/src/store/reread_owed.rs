use std::collections::BTreeSet;

use rusqlite::{params, Connection};

use crate::identity::{ModelKey, RowId, RowRef};
use crate::store::{CommitResult, RowChange, RowChangeKind};

use super::row_write::RowWrite;
use super::StoreError;

fn fault(error: impl std::fmt::Display) -> StoreError { StoreError::Constraint(error.to_string()) }

/// The row changed for the readers without a new revision: they read it again and see the debt.
fn announced(model: &str, id: &str) -> Result<CommitResult, StoreError> {
    let mut result = CommitResult::noop(uuid::Uuid::new_v4());
    result.rows.push(RowChange { row: RowRef::new(ModelKey::new(model), RowId::new(id).map_err(fault)?), kind: RowChangeKind::Touched, fields: BTreeSet::new() });
    Ok(result)
}

/// The event of a `via: :node` read owes the row a reread: nothing is written when the row is not stored or the debt stands.
pub(super) fn owe(writer: &mut RowWrite<'_>, model: &str, id: &str) -> Result<Option<CommitResult>, StoreError> {
    if writer.stored(model, id)?.is_none() { return Ok(None); }
    if writer.connection.execute(include_str!("sql/reread_owed_insert.sql"), params![model, id])? == 0 { return Ok(None); }
    announced(model, id).map(Some)
}

/// A server source wrote the row or the `node` window of the row landed: the debt is paid.
pub(super) fn clear(connection: &Connection, model: &str, id: &str) -> Result<bool, StoreError> {
    Ok(connection.execute(include_str!("sql/reread_owed_delete.sql"), params![model, id])? > 0)
}

pub(crate) fn is_owed(connection: &Connection, model: &str, id: &str) -> Result<bool, StoreError> {
    Ok(connection.query_row(include_str!("sql/reread_owed_exists.sql"), params![model, id], |row| row.get(0))?)
}

/// The rows of `ids` that owe a reread: one walk of the owed rows of the model.
pub(crate) fn owed_among(connection: &Connection, model: &str, ids: &[String]) -> Result<BTreeSet<String>, StoreError> {
    let owed = all_owed(connection, model)?;
    if owed.is_empty() { return Ok(owed); }
    let shown: BTreeSet<&String> = ids.iter().collect();
    Ok(owed.into_iter().filter(|id| shown.contains(id)).collect())
}

impl super::Store {
    /// The rows in debt a count or a local read depends on (D-393): the members of the counted window, or every row of the model for a read without a window. `None` for a read whose value rows tell it.
    pub(crate) fn owed_dependencies(&self, view: &crate::owners::read::deliveries::View) -> Result<Option<BTreeSet<String>>, super::db_error::CoreError> {
        use crate::owners::read::deliveries::Kind;
        use crate::owners::read::windows::WindowKind;
        let model = view.model.as_str();
        let scope = match (view.remote, view.kind) {
            (true, Kind::Count) => match view.window.kind() {
                WindowKind::Relation { relation, params } => {
                    let spec = self.spec(model)?;
                    let remote = spec.relations.iter().find(|declared| declared.name == relation).and_then(|declared| declared.remote.as_ref());
                    let Some(remote) = remote else { return Ok(None) };
                    Some(super::window_identity(spec, model, &relation, remote, &params)?.0)
                }
                WindowKind::Nodes { .. } => return Ok(None),
            },
            (false, Kind::List | Kind::Count | Kind::Entity { .. }) => None,
            _ => return Ok(None),
        };
        Ok(Some(self.read(&[model], |reader| {
            let all = all_owed(reader.connection(), model)?;
            match &scope {
                None => Ok(all),
                Some(scope) => all.into_iter().filter_map(|id| match reader.scope_has_member(model, scope.key.as_str(), &id) { Ok(true) => Some(Ok(id)), Ok(false) => None, Err(error) => Some(Err(error)) }).collect(),
            }
        })?))
    }
}

/// Every row of the model that owes a reread.
pub(crate) fn all_owed(connection: &Connection, model: &str) -> Result<BTreeSet<String>, StoreError> {
    let mut statement = connection.prepare_cached(include_str!("sql/reread_owed_model.sql"))?;
    let owed = statement.query_map([model], |row| row.get::<_, String>(0))?.collect::<rusqlite::Result<BTreeSet<String>>>()?;
    Ok(owed)
}
