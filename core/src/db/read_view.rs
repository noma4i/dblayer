//! The reads of a stream: the query of its description and the ids with their stamps at one revision of the snapshot.

use serde_json::{Map, Value};

use crate::query::{compile_count, fetch_count, Query};

use super::error::DbError;
use super::open::Db;

/// A query with the relation of its window: `Some((name, params))` when the relation is remote and watched.
pub(crate) type ParsedQuery = (Query, Option<(String, Map<String, Value>)>);

/// What a read of a stream gives: the ids with their stamps in order, or the count.
pub(crate) enum ViewRead {
    Rows(Vec<(String, i64)>),
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
    pub(crate) fn read_view(&self, query: &Query, count: bool, floor: i64) -> Result<(i64, ViewRead), DbError> {
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
            Ok((seen, ViewRead::Rows(self.shared.query_keys(reader, query)?)))
        })
    }
}
