use std::collections::BTreeSet;

use rusqlite::{params, Connection, OptionalExtension};

use crate::commit::{RowChange, RowChangeKind, ScopeRef};
use crate::identity::RowId;
use crate::query::{compile_ranked, fetch_ranked, place_of, Order, Query};
use crate::schema::SortField;
use crate::value::{fractional_between, fractional_keys_between, ORDER_KEY_LIMIT};

use super::rows::CHUNK;
use super::{ModelInfo, StoreError};

const REBALANCE_SPAN: usize = 8;
const PARKED: &str = "~parked~";

fn orders_of(fields: &[SortField]) -> Vec<Order> {
    fields.iter().map(|field| Order::new(&field.column, field.direction)).collect()
}

fn invalid(error: impl ToString) -> StoreError {
    StoreError::Constraint(error.to_string())
}

fn places_of(connection: &Connection, info: &ModelInfo, fields: &[SortField], ids: &[String]) -> Result<Vec<(String, String)>, StoreError> {
    let query = Query::new(info.spec.key.as_str()).ordered(orders_of(fields));
    let statement = compile_ranked(&query, &info.spec, Some(ids)).map_err(invalid)?;
    let found = fetch_ranked(connection, &statement.statement).map_err(invalid)?;
    Ok(found.into_iter().map(|record| (record.id, place_of(&record.cells, &statement.descending))).collect())
}

fn key_text(connection: &Connection, sql: &str, scope: &ScopeRef, bound: Option<&str>) -> Result<Option<String>, StoreError> {
    let mut statement = connection.prepare_cached(sql)?;
    let found = match bound {
        Some(bound) => statement.query_row(params![scope.model.as_str(), scope.key.as_str(), bound], |row| row.get(0)),
        None => statement.query_row(params![scope.model.as_str(), scope.key.as_str()], |row| row.get(0)),
    };
    Ok(found.optional()?.flatten())
}

fn neighbours(connection: &Connection, scope: &ScopeRef, row: &RowId, place: Option<&str>) -> Result<(Option<String>, Option<String>), StoreError> {
    let Some(place) = place.filter(|_| !row.is_temp()) else {
        return Ok((key_text(connection, "SELECT MAX(order_key) FROM scope_member WHERE model = ?1 AND scope_key = ?2", scope, None)?, None));
    };
    let lower: Option<String> = connection
        .prepare_cached(
            "SELECT order_key FROM scope_member WHERE model = ?1 AND scope_key = ?2 AND place IS NOT NULL AND (place, row_id) < (?3, ?4) \
             ORDER BY place DESC, row_id DESC LIMIT 1",
        )?
        .query_row(params![scope.model.as_str(), scope.key.as_str(), place, row.as_str()], |found| found.get(0))
        .optional()?;
    let upper = match &lower {
        Some(lower) => key_text(connection, "SELECT MIN(order_key) FROM scope_member WHERE model = ?1 AND scope_key = ?2 AND order_key > ?3", scope, Some(lower))?,
        None => key_text(connection, "SELECT MIN(order_key) FROM scope_member WHERE model = ?1 AND scope_key = ?2", scope, None)?,
    };
    Ok((lower, upper))
}

fn side(connection: &Connection, scope: &ScopeRef, bound: Option<&str>, above: bool) -> Result<Vec<(String, String)>, StoreError> {
    let Some(bound) = bound else { return Ok(Vec::new()) };
    let (comparison, direction) = if above { (">=", "ASC") } else { ("<=", "DESC") };
    let sql = format!(
        "SELECT row_id, order_key FROM scope_member WHERE model = ?1 AND scope_key = ?2 AND order_key {comparison} ?3 ORDER BY order_key {direction} LIMIT {}",
        REBALANCE_SPAN + 1
    );
    let mut statement = connection.prepare_cached(&sql)?;
    let rows = statement.query_map(params![scope.model.as_str(), scope.key.as_str(), bound], |row| Ok((row.get(0)?, row.get(1)?)))?;
    Ok(rows.collect::<Result<_, _>>()?)
}

fn rebalanced(connection: &Connection, scope: &ScopeRef, lower: Option<&str>, upper: Option<&str>, moved: &mut BTreeSet<String>) -> Result<String, StoreError> {
    let mut below = side(connection, scope, lower, false)?;
    let mut above = side(connection, scope, upper, true)?;
    let outer = |side: &mut Vec<(String, String)>| (side.len() > REBALANCE_SPAN).then(|| side.pop().map(|(_, key)| key)).flatten();
    let (outer_lower, outer_upper) = (outer(&mut below), outer(&mut above));
    below.reverse();
    let keys = fractional_keys_between(outer_lower.as_deref(), outer_upper.as_deref(), below.len() + 1 + above.len()).map_err(invalid)?;
    let members: Vec<&(String, String)> = below.iter().chain(&above).collect();
    for (row_id, _) in &members {
        connection.execute(
            "UPDATE scope_member SET order_key = ?1 WHERE model = ?2 AND scope_key = ?3 AND row_id = ?4",
            params![format!("{PARKED}{row_id}"), scope.model.as_str(), scope.key.as_str(), row_id],
        )?;
    }
    let mut assigned = keys.iter();
    for (row_id, _) in &below {
        set_key(connection, scope, row_id, assigned.next().ok_or_else(|| invalid("short run of keys"))?)?;
        moved.insert(row_id.clone());
    }
    let fresh = assigned.next().ok_or_else(|| invalid("short run of keys"))?.clone();
    for (row_id, _) in &above {
        set_key(connection, scope, row_id, assigned.next().ok_or_else(|| invalid("short run of keys"))?)?;
        moved.insert(row_id.clone());
    }
    Ok(fresh)
}

fn set_key(connection: &Connection, scope: &ScopeRef, row_id: &str, key: &str) -> Result<(), StoreError> {
    connection.execute(
        "UPDATE scope_member SET order_key = ?1 WHERE model = ?2 AND scope_key = ?3 AND row_id = ?4",
        params![key, scope.model.as_str(), scope.key.as_str(), row_id],
    )?;
    Ok(())
}

pub(super) struct Joined {
    pub joined: bool,
    pub moved: BTreeSet<String>,
}

pub(super) fn join(connection: &Connection, info: &ModelInfo, fields: &[SortField], scope: &ScopeRef, row: &RowId) -> Result<Joined, StoreError> {
    let member: Option<i64> = connection
        .prepare_cached("SELECT 1 FROM scope_member WHERE model = ?1 AND scope_key = ?2 AND row_id = ?3")?
        .query_row(params![scope.model.as_str(), scope.key.as_str(), row.as_str()], |found| found.get(0))
        .optional()?;
    let mut moved = BTreeSet::new();
    if member.is_some() {
        return Ok(Joined { joined: false, moved });
    }
    let place = places_of(connection, info, fields, &[row.to_string()])?.pop().map(|(_, place)| place);
    let (lower, upper) = neighbours(connection, scope, row, place.as_deref())?;
    let mut key = fractional_between(lower.as_deref(), upper.as_deref()).map_err(invalid)?;
    if key.len() > ORDER_KEY_LIMIT {
        key = rebalanced(connection, scope, lower.as_deref(), upper.as_deref(), &mut moved)?;
    }
    connection.execute(
        "INSERT INTO scope_member (model, scope_key, row_id, order_key, place) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![scope.model.as_str(), scope.key.as_str(), row.as_str(), key, place],
    )?;
    Ok(Joined { joined: true, moved })
}

pub(super) fn refresh_places(connection: &Connection, info: &ModelInfo, changes: &[&RowChange]) -> Result<(), StoreError> {
    for relation in info.spec.relations.iter().filter(|relation| relation.order_stable) {
        let crate::schema::Sort::Fields(fields) = &relation.sort else { continue };
        let ids: Vec<String> = changes
            .iter()
            .filter(|change| change.kind == RowChangeKind::Inserted || fields.iter().any(|field| change.fields.contains(&field.column)))
            .map(|change| change.row.id.to_string())
            .collect();
        let prefix = crate::identity::ListKey::compose(&[info.spec.key.as_str(), relation.name.as_str()]);
        for chunk in ids.chunks(CHUNK) {
            for (row_id, place) in places_of(connection, info, fields, chunk)? {
                connection.prepare_cached(
                    "UPDATE scope_member SET place = ?1 WHERE model = ?2 AND row_id = ?3 AND substr(scope_key, 1, ?4) = ?5 AND place IS NOT ?1",
                )?.execute(params![place, info.spec.key.as_str(), row_id, prefix.as_str().chars().count() as i64, prefix.as_str()])?;
            }
        }
    }
    Ok(())
}
