//! The reads of a stream: the query of its description and the ids with their stamps at one revision of the snapshot.

use serde_json::{Map, Value};

use crate::query::{compile_count, fetch_count, Query};

use super::error::DbError;
use super::open::Db;

/// A query with the relation of its window: `Some((name, params))` when the relation is remote and watched.
pub(crate) type ParsedQuery = (Query, Option<(String, Map<String, Value>)>);

pub(crate) enum ViewRead {
    /// The rows and the total the server gave for their window.
    Rows(Vec<(String, i64, super::Row)>, Option<u64>),
    Count(i64),
}

fn fault(error: serde_json::Error) -> DbError { DbError::Internal(error.to_string()) }

impl Db {
    pub(crate) fn parse_query(&self, json: &str) -> Result<ParsedQuery, DbError> {
        let description: Value = serde_json::from_str(json).map_err(fault)?;
        let mut query: Query = serde_json::from_value(description.clone()).map_err(fault)?;
        let remote = description["relation"].as_str().map(|name| -> Result<_, DbError> {
            let params: Map<String, Value> = description.get("params").map(|value| serde_json::from_value(value.clone())).transpose().map_err(fault)?.unwrap_or_default();
            let relation_query = self.remote_relation_query(query.model.as_str(), name, &params)?;
            let watch = relation_query.is_some() || self.spec(query.model.as_str())?.relations.iter().any(|relation| relation.name == name && !relation.updates.is_empty());
            if let Some(relation_query) = relation_query {
                query.scope = relation_query.scope;
                query.stable = relation_query.stable;
                if query.order.is_empty() { query.order = relation_query.order; }
            }
            Ok(watch.then_some((name.to_string(), params)))
        }).transpose()?.flatten();
        Ok((query, remote))
    }

    /// Reads `query` in one snapshot that is not older than `floor`; gives the revision of the snapshot.
    pub(crate) fn read_view(&self, query: &Query, count: bool, floor: i64, window: Option<&crate::owners::read::windows::WindowKey>) -> Result<(i64, ViewRead), DbError> {
        let model = query.model.as_str();
        self.shared.read(&[model], |reader| {
            let seen = reader.revision_seq()?;
            if seen < floor {
                return Err(DbError::Internal(format!("the snapshot is at revision {seen}, older than {floor}")));
            }
            if count {
                let statement = compile_count(query, self.spec(model)?)?;
                return Ok((seen, ViewRead::Count(fetch_count(reader.connection(), &statement)?)));
            }
            let keys = self.shared.query_keys(reader, query)?;
            let rows = self.shared.rows_for_keys(reader, model, &keys)?;
            if rows.len() != keys.len() {
                return Err(DbError::Internal(format!("the snapshot at revision {seen} lost {} of {} rows of {model}", keys.len() - rows.len(), keys.len())));
            }
            let total = match window.map(crate::owners::read::windows::WindowKey::kind) {
                Some(crate::owners::read::windows::WindowKind::Relation { relation, params }) => {
                    let spec = self.spec(model)?;
                    let remote = spec.relations.iter().find(|declared| declared.name == relation).and_then(|declared| declared.remote.as_ref());
                    match remote {
                        Some(remote) => {
                            let (_, key) = super::land_window::window_identity(spec, model, &relation, remote, &params)?;
                            rusqlite::OptionalExtension::optional(reader.connection().query_row("SELECT total FROM query_window WHERE key = ?1", [key], |row| row.get::<_, Option<i64>>(0)))?.flatten().and_then(|total| u64::try_from(total).ok())
                        }
                        None => None,
                    }
                }
                _ => None,
            };
            Ok((seen, ViewRead::Rows(keys.into_iter().zip(rows).map(|((id, stamp), row)| (id, stamp, row)).collect(), total)))
        })
    }
}
