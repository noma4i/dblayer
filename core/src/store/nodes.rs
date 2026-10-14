use std::collections::HashMap;

use rusqlite::types::Value as SqlValue;
use rusqlite::{params_from_iter, Connection};
use serde_json::{Number, Value};

use super::rows::placeholders;
use super::StoreError;

const NULL: i64 = 0;
const BOOL: i64 = 1;
const INT: i64 = 2;
const REAL: i64 = 3;
const TEXT: i64 = 4;
const OBJECT: i64 = 5;
const ARRAY: i64 = 6;

struct Node {
    node: i64,
    parent: i64,
    pos: i64,
    name: Option<String>,
    kind: i64,
    whole: Option<i64>,
    fraction: Option<f64>,
    text: Option<String>,
}

fn flatten(value: &Value, parent: i64, pos: i64, name: Option<&str>, out: &mut Vec<Node>) {
    let node = out.len() as i64;
    let mut entry = Node { node, parent, pos, name: name.map(str::to_string), kind: NULL, whole: None, fraction: None, text: None };
    match value {
        Value::Null => {}
        Value::Bool(flag) => (entry.kind, entry.whole) = (BOOL, Some(i64::from(*flag))),
        Value::Number(number) => match number.as_i64() {
            Some(whole) => (entry.kind, entry.whole) = (INT, Some(whole)),
            None => (entry.kind, entry.fraction) = (REAL, number.as_f64()),
        },
        Value::String(text) => (entry.kind, entry.text) = (TEXT, Some(text.clone())),
        Value::Object(_) => entry.kind = OBJECT,
        Value::Array(_) => entry.kind = ARRAY,
    }
    out.push(entry);
    match value {
        Value::Object(fields) => fields.iter().enumerate().for_each(|(at, (key, child))| flatten(child, node, at as i64, Some(key), out)),
        Value::Array(items) => items.iter().enumerate().for_each(|(at, child)| flatten(child, node, at as i64, None, out)),
        _ => {}
    }
}

fn assemble(nodes: Vec<Node>) -> Option<Value> {
    let mut children: HashMap<i64, Vec<(Option<String>, Value)>> = HashMap::new();
    let mut root = None;
    for entry in nodes.into_iter().rev() {
        let mut mine = children.remove(&entry.node).unwrap_or_default();
        mine.reverse();
        let value = match entry.kind {
            NULL => Value::Null,
            BOOL => Value::Bool(entry.whole? != 0),
            INT => Value::from(entry.whole?),
            REAL => Number::from_f64(entry.fraction?).map_or(Value::Null, Value::Number),
            TEXT => Value::String(entry.text?),
            OBJECT => Value::Object(mine.into_iter().filter_map(|(name, child)| name.map(|name| (name, child))).collect()),
            ARRAY => Value::Array(mine.into_iter().map(|(_, child)| child).collect()),
            _ => return None,
        };
        if entry.parent < 0 {
            root = Some(value);
        } else {
            children.entry(entry.parent).or_default().push((entry.name, value));
        }
    }
    root
}

pub(crate) fn write_tree(connection: &Connection, table: &str, keys: &[&str], owner: &[String], value: &Value) -> Result<(), StoreError> {
    let mut nodes = Vec::new();
    flatten(value, -1, 0, None, &mut nodes);
    let names: Vec<&str> = keys.iter().copied().chain(["node", "parent", "pos", "name", "kind", "whole", "fraction", "text"]).collect();
    let mut statement = connection.prepare_cached(&format!("INSERT INTO {table} ({}) VALUES ({})", names.join(", "), placeholders(names.len())))?;
    for entry in nodes {
        let cells = owner.iter().map(|part| SqlValue::Text(part.clone())).chain([
            SqlValue::Integer(entry.node),
            SqlValue::Integer(entry.parent),
            SqlValue::Integer(entry.pos),
            entry.name.map_or(SqlValue::Null, SqlValue::Text),
            SqlValue::Integer(entry.kind),
            entry.whole.map_or(SqlValue::Null, SqlValue::Integer),
            entry.fraction.map_or(SqlValue::Null, SqlValue::Real),
            entry.text.map_or(SqlValue::Null, SqlValue::Text),
        ]);
        statement.execute(params_from_iter(cells))?;
    }
    Ok(())
}

pub(crate) fn delete_trees(connection: &Connection, table: &str, filter: &str, params: &[String]) -> Result<(), StoreError> {
    connection.prepare_cached(&format!("DELETE FROM {table} WHERE {filter}"))?.execute(params_from_iter(params.iter()))?;
    Ok(())
}

pub(crate) fn read_trees(connection: &Connection, table: &str, keys: &[&str], filter: &str, params: &[String]) -> Result<Vec<(Vec<String>, Value)>, StoreError> {
    let order = keys.join(", ");
    let sql = format!("SELECT {order}, node, parent, pos, name, kind, whole, fraction, text FROM {table} WHERE {filter} ORDER BY {order}, node");
    let mut statement = connection.prepare_cached(&sql)?;
    let count = keys.len();
    let rows = statement.query_map(params_from_iter(params.iter()), |row| {
        let owner = (0..count).map(|at| row.get::<_, String>(at)).collect::<Result<Vec<_>, _>>()?;
        let node = Node {
            node: row.get(count)?,
            parent: row.get(count + 1)?,
            pos: row.get(count + 2)?,
            name: row.get(count + 3)?,
            kind: row.get(count + 4)?,
            whole: row.get(count + 5)?,
            fraction: row.get(count + 6)?,
            text: row.get(count + 7)?,
        };
        Ok((owner, node))
    })?;
    let mut found: Vec<(Vec<String>, Vec<Node>)> = Vec::new();
    for item in rows {
        let (owner, node) = item?;
        match found.last_mut() {
            Some((last, nodes)) if *last == owner => nodes.push(node),
            _ => found.push((owner, vec![node])),
        }
    }
    Ok(found.into_iter().filter_map(|(owner, nodes)| assemble(nodes).map(|value| (owner, value))).collect())
}

pub(crate) fn save_pending(connection: &Connection, model: &str, row_id: &str, path: &str, value: &Value) -> Result<(), StoreError> {
    let owner = [model.to_string(), row_id.to_string(), path.to_string()];
    delete_trees(connection, "local_pending", "model = ?1 AND row_id = ?2 AND path = ?3", &owner)?;
    write_tree(connection, "local_pending", &["model", "row_id", "path"], &owner, value)
}
