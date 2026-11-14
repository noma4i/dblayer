use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, Connection};

use crate::identity::{ModelKey, RowId, RowRef};
use crate::store::{CommitResult, RowChange, RowChangeKind};

use super::row_write::RowWrite;
use super::window_write::fault;
use super::StoreError;

fn announced(model: &str, id: &str) -> Result<CommitResult, StoreError> {
    let mut result = CommitResult::noop(uuid::Uuid::new_v4());
    announce(&mut result, model, id)?;
    Ok(result)
}

/// The event of a `via: :node` read owes the row a reread by that read at the revision of its batch. `None` when the row is not stored. A debt that stands is raised to the new revision without announcing the row.
pub(super) fn owe(writer: &mut RowWrite<'_>, model: &str, id: &str, relation: &str) -> Result<Option<Owed>, StoreError> {
    if writer.stored(model, id)?.is_none() { return Ok(None); }
    let stands = is_owed(writer.connection, model, id, relation)?;
    writer.connection.execute(include_str!("sql/reread_owed_insert.sql"), params![model, id, relation, writer.revision])?;
    Ok(Some(if stands { Owed::Raised } else { Owed::New(announced(model, id)?) }))
}

pub(super) enum Owed { New(CommitResult), Raised }

#[derive(Clone, Copy)]
pub(super) enum Release<'a> {
    Landing(i64),
    Node { relation: &'a str, base: i64 },
    Event,
    Deleted,
}

impl Release<'_> {
    pub(super) fn of(source: crate::schema::RowSource, request_base: Option<i64>) -> Option<Release<'static>> {
        use crate::schema::RowSource;
        match source {
            RowSource::Page | RowSource::NodeReread => request_base.map(Release::Landing),
            RowSource::Event => Some(Release::Event),
            RowSource::Response | RowSource::Create | RowSource::Local | RowSource::Derived => None,
        }
    }
}

pub(super) fn release(connection: &Connection, model: &str, id: &str, release: Release<'_>) -> Result<bool, StoreError> {
    let changed = match release {
        Release::Node { relation, base } => connection.execute(include_str!("sql/reread_owed_delete_read.sql"), params![model, id, relation, base])?,
        Release::Landing(base) => connection.execute(include_str!("sql/reread_owed_delete.sql"), params![model, id, base])?,
        Release::Event | Release::Deleted => connection.execute(include_str!("sql/reread_owed_delete.sql"), params![model, id, None::<i64>])?,
    };
    Ok(changed > 0)
}

pub(super) fn announce(result: &mut CommitResult, model: &str, id: &str) -> Result<(), StoreError> {
    let row = RowRef::new(ModelKey::new(model), RowId::new(id).map_err(fault)?);
    if !result.rows.iter().any(|change| change.row == row) { result.rows.push(RowChange { row, kind: RowChangeKind::Touched, fields: BTreeSet::new() }); }
    Ok(())
}

pub(crate) fn is_owed(connection: &Connection, model: &str, id: &str, relation: &str) -> Result<bool, StoreError> {
    Ok(connection.query_row(include_str!("sql/reread_owed_exists.sql"), params![model, id, relation], |row| row.get(0))?)
}

/// The debts a view depends on: (model, row, read) of the shown rows, of the rows its nested references read, and of the children of its derived fields.
type Owing = (String, String, String);

enum Shown {
    Row(String),
    Scope(String),
    Table,
}

fn pointer_of(row: &serde_json::Map<String, serde_json::Value>, field: &str) -> Option<String> { row.get(field).and_then(serde_json::Value::as_str).map(str::to_string) }

impl super::Store {
    /// The models whose rows set derived fields of `parent`, with the pointer field of each.
    pub(crate) fn derived_children(&self, parent: &str) -> Vec<(String, String)> {
        let mut found = Vec::new();
        for child in self.specs().values() {
            found.extend(child.associations.iter().filter(|association| association.kind == crate::schema::AssociationKind::BelongsTo && !association.sets.is_empty() && association.target.as_str() == parent).map(|association| (child.key.to_string(), association.foreign_key.clone())));
        }
        found
    }

    /// The models other than `model` whose rows a read of `model` depends on: those its nested references read and the children of derived fields.
    pub(crate) fn dependency_models(&self, model: &str) -> Vec<String> {
        let targets = self.spec(model).map(super::plan::nested_ref::link_targets).unwrap_or_default();
        let shown: Vec<String> = std::iter::once(model.to_string()).chain(targets).collect();
        let children: Vec<String> = shown.iter().flat_map(|name| self.derived_children(name).into_iter().map(|(child, _)| child)).collect();
        let mut found: Vec<String> = shown.into_iter().skip(1).chain(children).filter(|name| name != model).collect();
        found.sort();
        found.dedup();
        found
    }

    pub(crate) fn owed_dependencies(&self, view: &crate::owners::read::deliveries::View, shown_rows: &[String]) -> Result<Option<BTreeSet<Owing>>, super::db_error::CoreError> {
        use crate::owners::read::deliveries::Kind;
        use crate::owners::read::windows::WindowKind;
        let model = view.model.as_str();
        let spec = self.spec(model)?;
        let shown = match view.kind {
            Kind::Entity { .. } => Shown::Row(view.subject.clone()),
            Kind::List | Kind::Count if view.remote => match view.window.kind().map_err(StoreError::Constraint)? {
                WindowKind::Relation { relation, params } => {
                    let remote = spec.relations.iter().find(|declared| declared.name == relation).and_then(|declared| declared.remote.as_ref());
                    match remote {
                        Some(remote) => Shown::Scope(super::window_identity(spec, model, &relation, remote, &params)?.0.key.as_str().to_string()),
                        None => Shown::Table,
                    }
                }
                WindowKind::Nodes { .. } => Shown::Table,
            },
            Kind::List | Kind::Count => Shown::Table,
            _ => return Ok(None),
        };
        let targets = super::plan::nested_ref::link_targets(spec);
        let models: Vec<&str> = std::iter::once(model).chain(targets.iter().map(String::as_str)).collect();
        Ok(Some(self.read_models(&models, |reader| {
            let tracked: BTreeSet<String> = std::iter::once(model.to_string()).chain(self.dependency_models(model)).collect();
            let debts = {
                let mut statement = reader.connection().prepare_cached(include_str!("sql/reread_owed_all.sql"))?;
                let all = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)))?.collect::<rusqlite::Result<Vec<Owing>>>()?;
                all.into_iter().filter(|(owed, _, _)| tracked.contains(owed)).collect::<Vec<Owing>>()
            };
            let mut found = BTreeSet::new();
            if debts.is_empty() { return Ok(found); }
            let ids: Option<BTreeSet<String>> = match &shown {
                Shown::Row(id) => Some(BTreeSet::from([id.clone()])),
                Shown::Scope(key) => {
                    let mut statement = reader.connection().prepare_cached(include_str!("sql/reread_owed_member_ids.sql"))?;
                    let members = statement.query_map(params![model, key], |row| row.get::<_, String>(0))?.collect::<rusqlite::Result<BTreeSet<String>>>()?;
                    Some(members)
                }
                Shown::Table => None,
            };
            found.extend(debts.iter().filter(|(owed, id, _)| owed == model && ids.as_ref().is_none_or(|ids| ids.contains(id))).cloned());
            if view.kind == Kind::Count { return Ok(found); }
            let mut parents: BTreeMap<String, Option<BTreeSet<String>>> = BTreeMap::new();
            if spec.fields.iter().any(|field| field.derived) { parents.insert(model.to_string(), ids.clone()); }
            if !targets.is_empty() && debts.iter().any(|(owed, _, _)| owed != model) {
                let stored = reader.stored_rows(model, shown_rows)?;
                let mut linked: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
                for (target, id) in super::plan::nested_ref::linked_rows(spec, stored.values().map(|stored| &stored.row)) { linked.entry(target).or_default().insert(id); }
                found.extend(debts.iter().filter(|(owed, id, _)| linked.get(owed).is_some_and(|ids| ids.contains(id))).cloned());
                for (target, ids) in linked {
                    if self.spec(&target)?.fields.iter().any(|field| field.derived) { parents.insert(target, Some(ids)); }
                }
            }
            for (parent, ids) in &parents {
                for (child, foreign_key) in self.derived_children(parent) {
                    let children: Vec<&Owing> = debts.iter().filter(|(owed, _, _)| *owed == child).collect();
                    let wanted: Vec<String> = children.iter().map(|(_, id, _)| id.clone()).collect();
                    let stored = reader.stored_rows(&child, &wanted)?;
                    found.extend(children.into_iter().filter(|(_, id, _)| stored.get(id).and_then(|stored| pointer_of(&stored.row, &foreign_key)).is_some_and(|pointer| ids.as_ref().is_none_or(|ids| ids.contains(&pointer)))).cloned());
                }
            }
            Ok(found)
        })?))
    }
}
