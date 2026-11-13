use std::collections::BTreeSet;

use rusqlite::{params, Connection};

use crate::identity::{ModelKey, RowId, RowRef};
use crate::store::{CommitResult, RowChange, RowChangeKind};

use super::row_write::RowWrite;
use super::window_write::fault;
use super::StoreError;

fn announced(model: &str, id: &str) -> Result<CommitResult, StoreError> {
    let mut result = CommitResult::noop(uuid::Uuid::new_v4());
    result.rows.push(RowChange { row: RowRef::new(ModelKey::new(model), RowId::new(id).map_err(fault)?), kind: RowChangeKind::Touched, fields: BTreeSet::new() });
    Ok(result)
}

/// The event of a `via: :node` read owes the row a reread at the revision of its batch. `None` when the row is not stored. A debt that stands is raised to the new revision without announcing the row.
pub(super) fn owe(writer: &mut RowWrite<'_>, model: &str, id: &str) -> Result<Option<Owed>, StoreError> {
    if writer.stored(model, id)?.is_none() { return Ok(None); }
    let stands = writer.connection.query_row(include_str!("sql/reread_owed_exists.sql"), params![model, id], |row| row.get::<_, bool>(0))?;
    writer.connection.execute(include_str!("sql/reread_owed_insert.sql"), params![model, id, writer.revision])?;
    Ok(Some(if stands { Owed::Raised } else { Owed::New(announced(model, id)?) }))
}

pub(super) enum Owed { New(CommitResult), Raised }

#[derive(Clone, Copy)]
pub(super) enum Release {
    Landing(i64),
    Event,
    Deleted,
}

impl Release {
    pub(super) fn of(source: crate::schema::RowSource, request_base: Option<i64>) -> Option<Release> {
        use crate::schema::RowSource;
        match source {
            RowSource::Page | RowSource::NodeReread => request_base.map(Release::Landing),
            RowSource::Event => Some(Release::Event),
            RowSource::Response | RowSource::Create | RowSource::Local | RowSource::Derived => None,
        }
    }
}

pub(super) fn release(connection: &Connection, model: &str, id: &str, release: Release) -> Result<bool, StoreError> {
    let base = match release { Release::Landing(base) => Some(base), Release::Event | Release::Deleted => None };
    Ok(connection.execute(include_str!("sql/reread_owed_delete.sql"), params![model, id, base])? > 0)
}

pub(crate) fn is_owed(connection: &Connection, model: &str, id: &str) -> Result<bool, StoreError> {
    Ok(connection.query_row(include_str!("sql/reread_owed_exists.sql"), params![model, id], |row| row.get(0))?)
}

pub(crate) fn owed_among(connection: &Connection, model: &str, ids: &[String]) -> Result<BTreeSet<String>, StoreError> {
    if ids.is_empty() { return Ok(BTreeSet::new()); }
    let mut statement = connection.prepare_cached(include_str!("sql/reread_owed_among.sql"))?;
    let owed = statement.query_map(params![model, serde_json::to_string(ids)?], |row| row.get::<_, String>(0))?.collect::<rusqlite::Result<BTreeSet<String>>>()?;
    Ok(owed)
}

impl super::Store {
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
            match &scope {
                None => all_owed(reader.connection(), model),
                Some(scope) => members_owed(reader.connection(), model, scope.key.as_str()),
            }
        })?))
    }
}

pub(crate) fn all_owed(connection: &Connection, model: &str) -> Result<BTreeSet<String>, StoreError> {
    let mut statement = connection.prepare_cached(include_str!("sql/reread_owed_model.sql"))?;
    let owed = statement.query_map([model], |row| row.get::<_, String>(0))?.collect::<rusqlite::Result<BTreeSet<String>>>()?;
    Ok(owed)
}

fn members_owed(connection: &Connection, model: &str, scope: &str) -> Result<BTreeSet<String>, StoreError> {
    let mut statement = connection.prepare_cached(include_str!("sql/reread_owed_members.sql"))?;
    let owed = statement.query_map(params![model, scope], |row| row.get::<_, String>(0))?.collect::<rusqlite::Result<BTreeSet<String>>>()?;
    Ok(owed)
}
