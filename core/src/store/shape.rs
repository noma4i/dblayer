use std::collections::{BTreeMap, HashMap};

use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{params_from_iter, Connection};
use serde_json::{Map, Number, Value};

use crate::ddl::{Layout, Leaf, Member, Table, TableShape};
use crate::value::{read_array, read_enum, read_scalar};

use super::rows::{column_value, placeholders};
use super::StoreError;

pub(crate) type Cells = Vec<SqlValue>;

pub(crate) fn normalize_leaf(leaf: &Leaf, value: &Value) -> Option<Value> {
    match (&leaf.scalar, &leaf.values) {
        (Some(kind), _) => read_scalar(value, *kind),
        (None, Some(values)) => read_enum(value, values).map(Value::String),
        _ => None,
    }
}

fn normalize_members(members: &[Member], layout: &Layout, source: &Map<String, Value>) -> Map<String, Value> {
    members.iter().filter_map(|member| source.get(member.key()).and_then(|value| normalize(member, layout, value)).map(|value| (member.key().to_string(), value))).collect()
}

/// The value as storage returns it: known members only, scalars read by their kind, nulls and wrong types removed.
pub(crate) fn normalize(member: &Member, layout: &Layout, value: &Value) -> Option<Value> {
    match member {
        Member::Leaf(leaf) => normalize_leaf(leaf, value),
        Member::Object { members, .. } => value.as_object().map(|source| Value::Object(normalize_members(members, layout, source))),
        Member::List { table, .. } => match &layout.tables[*table].shape {
            TableShape::Values(leaf) => read_array(value, |item| normalize_leaf(leaf, item)).map(Value::Array),
            TableShape::Elements(_) => None,
        },
        Member::Array { table, .. } => match &layout.tables[*table].shape {
            TableShape::Elements(members) => value.as_array().map(|items| {
                Value::Array(items.iter().filter_map(Value::as_object).map(|item| Value::Object(normalize_members(members, layout, item))).collect())
            }),
            TableShape::Values(_) => None,
        },
    }
}

fn leaf_sql(leaf: &Leaf, value: Option<&Value>) -> SqlValue {
    column_value(leaf.ty, value)
}

pub(crate) fn member_values(member: &Member, value: Option<&Value>, out: &mut Vec<(String, SqlValue)>) {
    let present = |flag: bool| if flag { SqlValue::Integer(1) } else { SqlValue::Null };
    match member {
        Member::Leaf(leaf) => out.push((leaf.column.clone(), leaf_sql(leaf, value))),
        Member::Object { present: column, members, .. } => {
            let source = value.and_then(Value::as_object);
            out.push((column.clone(), present(source.is_some())));
            for child in members {
                member_values(child, source.and_then(|source| source.get(child.key())), out);
            }
        }
        Member::List { present: column, .. } | Member::Array { present: column, .. } => out.push((column.clone(), present(value.is_some_and(Value::is_array)))),
    }
}

pub(crate) fn delete_tables(connection: &Connection, layout: &Layout, tables: &[usize], row_id: &str) -> Result<(), StoreError> {
    for index in tables {
        connection.prepare_cached(&format!("DELETE FROM {} WHERE row_id = ?1", layout.tables[*index].name))?.execute([row_id])?;
    }
    Ok(())
}

const ITEMS_PER_STATEMENT: usize = 200;

pub(crate) fn insert_children(
    connection: &Connection,
    layout: &Layout,
    member: &Member,
    value: Option<&Value>,
    row_id: &str,
    owner: i64,
    nodes: &mut Vec<i64>,
) -> Result<(), StoreError> {
    match member {
        Member::Leaf(_) => {}
        Member::Object { members, .. } => {
            let source = value.and_then(Value::as_object);
            for child in members {
                insert_children(connection, layout, child, source.and_then(|source| source.get(child.key())), row_id, owner, nodes)?;
            }
        }
        Member::List { table, .. } => {
            let (Some(items), TableShape::Values(leaf)) = (value.and_then(Value::as_array), &layout.tables[*table].shape) else { return Ok(()) };
            let name = &layout.tables[*table].name;
            let positions: Vec<usize> = (0..items.len()).collect();
            for chunk in positions.chunks(ITEMS_PER_STATEMENT) {
                let sql = format!("INSERT INTO {name} (row_id, owner, pos, value) VALUES {}", vec!["(?, ?, ?, ?)"; chunk.len()].join(", "));
                let cells = chunk.iter().flat_map(|position| [SqlValue::Text(row_id.to_string()), owner.into(), (*position as i64).into(), leaf_sql(leaf, Some(&items[*position]))]);
                connection.prepare_cached(&sql)?.execute(params_from_iter(cells))?;
            }
        }
        Member::Array { table, .. } => {
            let (Some(items), TableShape::Elements(members)) = (value.and_then(Value::as_array), &layout.tables[*table].shape) else { return Ok(()) };
            for (position, item) in items.iter().enumerate() {
                nodes[*table] += 1;
                let node = nodes[*table];
                let mut columns: Vec<(String, SqlValue)> = Vec::new();
                for child in members {
                    member_values(child, item.as_object().and_then(|source| source.get(child.key())), &mut columns);
                }
                let names: Vec<&str> = ["row_id", "node", "owner", "pos"].into_iter().chain(columns.iter().map(|(name, _)| name.as_str())).collect();
                let sql = format!("INSERT INTO {} ({}) VALUES ({})", layout.tables[*table].name, names.join(", "), placeholders(names.len()));
                let head = [SqlValue::Text(row_id.to_string()), node.into(), owner.into(), (position as i64).into()];
                connection.prepare_cached(&sql)?.execute(params_from_iter(head.into_iter().chain(columns.into_iter().map(|(_, value)| value))))?;
                for child in members {
                    insert_children(connection, layout, child, item.as_object().and_then(|source| source.get(child.key())), row_id, node, nodes)?;
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn take_leaf(leaf: &Leaf, cells: &mut Cells) -> Option<Value> {
    match std::mem::replace(cells.get_mut(leaf.pos)?, SqlValue::Null) {
        SqlValue::Text(text) => Some(Value::String(text)),
        other => leaf_value(leaf, Some(&other)),
    }
}

pub(crate) fn leaf_value(leaf: &Leaf, cell: Option<&SqlValue>) -> Option<Value> {
    match (cell?, leaf.ty, leaf.is_bool) {
        (SqlValue::Text(text), _, _) => Some(Value::String(text.clone())),
        (SqlValue::Integer(number), _, true) => Some(Value::Bool(*number != 0)),
        (SqlValue::Integer(number), _, false) => Some(Value::from(*number)),
        (SqlValue::Real(number), _, _) => Number::from_f64(*number).map(Value::Number),
        _ => None,
    }
}

struct Element {
    node: i64,
    cells: Cells,
}

#[derive(Default)]
pub(crate) struct TableRows {
    values: HashMap<i64, Vec<(i64, SqlValue)>>,
    elements: HashMap<i64, Vec<(i64, Element)>>,
}

pub(crate) type RowTables = Vec<TableRows>;

fn present(cells: &Cells, at: usize) -> bool {
    !matches!(cells.get(at), None | Some(SqlValue::Null))
}

pub(crate) fn assemble(member: &Member, layout: &Layout, cells: &Cells, tables: &RowTables, owner: i64) -> Option<Value> {
    match member {
        Member::Leaf(leaf) => leaf_value(leaf, cells.get(leaf.pos)),
        Member::Object { at, members, .. } => {
            if !present(cells, *at) {
                return None;
            }
            Some(Value::Object(assemble_members(members, layout, cells, tables, owner)))
        }
        Member::List { at, table, .. } => {
            if !present(cells, *at) {
                return None;
            }
            let TableShape::Values(leaf) = &layout.tables[*table].shape else { return None };
            let items = tables[*table].values.get(&owner).map(Vec::as_slice).unwrap_or_default();
            Some(Value::Array(items.iter().filter_map(|(_, value)| leaf_value(leaf, Some(value))).collect()))
        }
        Member::Array { at, table, .. } => {
            if !present(cells, *at) {
                return None;
            }
            let TableShape::Elements(members) = &layout.tables[*table].shape else { return None };
            let items = tables[*table].elements.get(&owner).map(Vec::as_slice).unwrap_or_default();
            Some(Value::Array(items.iter().map(|(_, element)| Value::Object(assemble_members(members, layout, &element.cells, tables, element.node))).collect()))
        }
    }
}

fn assemble_members(members: &[Member], layout: &Layout, cells: &Cells, tables: &RowTables, owner: i64) -> Map<String, Value> {
    members.iter().filter_map(|member| assemble(member, layout, cells, tables, owner).map(|value| (member.key().to_string(), value))).collect()
}

pub(crate) fn read_cells(row: &rusqlite::Row<'_>, count: usize, skip: usize) -> rusqlite::Result<Cells> {
    let mut cells = Cells::with_capacity(count);
    for offset in 0..count {
        cells.push(match row.get_ref(skip + offset)? {
            ValueRef::Null => SqlValue::Null,
            ValueRef::Integer(number) => SqlValue::Integer(number),
            ValueRef::Real(number) => SqlValue::Real(number),
            ValueRef::Text(text) => SqlValue::Text(String::from_utf8_lossy(text).into_owned()),
            ValueRef::Blob(bytes) => SqlValue::Blob(bytes.to_vec()),
        });
    }
    Ok(cells)
}

fn read_table(connection: &Connection, table: &Table, ids: &[String]) -> Result<BTreeMap<String, TableRows>, StoreError> {
    let mut found: BTreeMap<String, TableRows> = BTreeMap::new();
    match &table.shape {
        TableShape::Values(_) => {
            let sql = format!("SELECT row_id, owner, pos, value FROM {} WHERE row_id IN ({}) ORDER BY row_id, owner, pos", table.name, placeholders(ids.len()));
            let mut statement = connection.prepare_cached(&sql)?;
            let rows = statement.query_map(params_from_iter(ids.iter()), |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, SqlValue>(3)?)))?;
            for item in rows {
                let (row_id, owner, position, value) = item?;
                found.entry(row_id).or_default().values.entry(owner).or_default().push((position, value));
            }
        }
        TableShape::Elements(members) => {
            let mut columns = Vec::new();
            members.iter().for_each(|member| crate::ddl::member_columns(member, &mut columns));
            let names: Vec<String> = columns.into_iter().map(|(name, _)| name).collect();
            let list: String = names.iter().map(|name| format!(", {name}")).collect();
            let sql = format!("SELECT row_id, node, owner, pos{list} FROM {} WHERE row_id IN ({}) ORDER BY row_id, owner, pos", table.name, placeholders(ids.len()));
            let mut statement = connection.prepare_cached(&sql)?;
            let rows = statement.query_map(params_from_iter(ids.iter()), |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, read_cells(row, names.len(), 4)?)))?;
            for item in rows {
                let (row_id, node, owner, position, cells) = item?;
                found.entry(row_id).or_default().elements.entry(owner).or_default().push((position, Element { node, cells }));
            }
        }
    }
    Ok(found)
}

/// Reads the child tables of one chunk of rows. A row without child rows gets empty tables.
pub(crate) fn read_tables(connection: &Connection, layout: &Layout, ids: &[String]) -> Result<HashMap<String, RowTables>, StoreError> {
    let mut per_row: HashMap<String, RowTables> = ids.iter().map(|id| (id.clone(), (0..layout.tables.len()).map(|_| TableRows::default()).collect())).collect();
    for (index, table) in layout.tables.iter().enumerate() {
        for (row_id, rows) in read_table(connection, table, ids)? {
            if let Some(tables) = per_row.get_mut(&row_id) {
                tables[index] = rows;
            }
        }
    }
    Ok(per_row)
}
