use crate::schema::{FieldKind, FieldSpec, ModelSpec};
use crate::value::ScalarKind;

use super::{column_name, column_type, rev_column, table_name, ColumnType};

#[derive(Debug, Clone, PartialEq)]
pub struct Leaf {
    pub key: String,
    pub column: String,
    pub pos: usize,
    pub ty: ColumnType,
    pub is_bool: bool,
    pub scalar: Option<ScalarKind>,
    pub values: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Member {
    Leaf(Leaf),
    Object { key: String, present: String, at: usize, members: Vec<Member> },
    List { key: String, present: String, at: usize, table: usize },
    Array { key: String, present: String, at: usize, table: usize },
}

impl Member {
    pub fn key(&self) -> &str {
        match self {
            Member::Leaf(leaf) => &leaf.key,
            Member::Object { key, .. } | Member::List { key, .. } | Member::Array { key, .. } => key,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableShape {
    Values(Leaf),
    Elements(Vec<Member>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub name: String,
    pub shape: TableShape,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldPlan {
    Leaf { field: String, leaf: Leaf },
    Typed { field: String, member: Member, tables: Vec<usize> },
    Rest { field: String },
}

impl FieldPlan {
    pub fn field(&self) -> &str {
        match self {
            FieldPlan::Leaf { field, .. } | FieldPlan::Typed { field, .. } | FieldPlan::Rest { field } => field,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Layout {
    pub table: String,
    pub fields: Vec<FieldPlan>,
    pub tables: Vec<Table>,
    row_columns: Vec<(String, ColumnType)>,
    revs: Vec<(String, String)>,
    pub select: String,
    pub select_lean: String,
}

struct Builder {
    row_table: String,
    tables: Vec<Table>,
    stack: Vec<(String, usize)>,
}

fn leaf(field: &FieldSpec, key: &str, column: &str) -> Option<Leaf> {
    let ty = column_type(&field.kind)?;
    let (scalar, values) = match &field.kind {
        FieldKind::Scalar { kind } => (Some(*kind), None),
        FieldKind::Enum { values } => (None, Some(values.clone())),
        _ => (None, None),
    };
    Some(Leaf { key: key.to_string(), column: column.to_string(), pos: 0, ty, is_bool: scalar == Some(ScalarKind::Bool), scalar, values })
}

impl Builder {
    fn member(&mut self, field: &FieldSpec, key: &str, base: &str, path: &str) -> Option<Member> {
        let present = format!("{base}__present");
        match &field.kind {
            FieldKind::Scalar { .. } | FieldKind::Enum { .. } => leaf(field, key, base).map(Member::Leaf),
            FieldKind::Object if field.app_type.is_some() => {
                let mut members = Vec::new();
                for child in &field.fields {
                    members.push(self.member(child, child.from.as_deref().unwrap_or(&child.name), &format!("{base}__{}", column_name(&child.name)), &format!("{path}__{}", column_name(&child.name)))?);
                }
                Some(Member::Object { key: key.to_string(), present, at: 0, members })
            }
            FieldKind::List { element } => {
                let value = leaf(&FieldSpec::scalar("value", *element), "value", "value")?;
                self.tables.push(Table { name: format!("{}__{path}", self.row_table), shape: TableShape::Values(value) });
                Some(Member::List { key: key.to_string(), present, at: 0, table: self.tables.len() - 1 })
            }
            FieldKind::Array if field.app_type.is_some() => {
                let name = field.app_type.clone().unwrap_or_default();
                if field.fields.is_empty() {
                    let table = self.stack.iter().rev().find(|(declared, _)| *declared == name)?.1;
                    return Some(Member::Array { key: key.to_string(), present, at: 0, table });
                }
                let index = self.tables.len();
                self.tables.push(Table { name: format!("{}__{path}", self.row_table), shape: TableShape::Elements(Vec::new()) });
                self.stack.push((name, index));
                let mut members = Vec::new();
                for child in &field.fields {
                    let column = format!("f_{}", column_name(&child.name));
                    members.push(self.member(child, child.from.as_deref().unwrap_or(&child.name), &column, &format!("{path}__{}", column_name(&child.name)))?);
                }
                self.stack.pop();
                self.tables[index].shape = TableShape::Elements(members);
                Some(Member::Array { key: key.to_string(), present, at: 0, table: index })
            }
            _ => None,
        }
    }
}

pub fn layout(model: &ModelSpec) -> Layout {
    let mut builder = Builder { row_table: table_name(model.key.as_str()), tables: Vec::new(), stack: Vec::new() };
    let mut fields = Vec::new();
    for field in model.fields.iter().filter(|field| field.name != "id") {
        let base = column_name(&field.name);
        if let Some(leaf) = leaf(field, &field.name, &base) {
            fields.push(FieldPlan::Leaf { field: field.name.clone(), leaf });
            continue;
        }
        let before = builder.tables.len();
        let typed = if field.is_structured() && !field.has_free_part() { builder.member(field, &field.name, &base, &base) } else { None };
        match typed {
            Some(member) => fields.push(FieldPlan::Typed { field: field.name.clone(), member, tables: (before..builder.tables.len()).collect() }),
            None => {
                builder.tables.truncate(before);
                builder.stack.clear();
                fields.push(FieldPlan::Rest { field: field.name.clone() });
            }
        }
    }
    let mut tables = builder.tables;
    let mut next = 0;
    for plan in &mut fields {
        match plan {
            FieldPlan::Leaf { leaf, .. } => {
                leaf.pos = next;
                next += 1;
            }
            FieldPlan::Typed { member, .. } => number(member, &mut next),
            FieldPlan::Rest { .. } => {}
        }
    }
    for table in &mut tables {
        if let TableShape::Elements(members) = &mut table.shape {
            let mut next = 0;
            members.iter_mut().for_each(|member| number(member, &mut next));
        }
    }
    let mut layout = Layout { table: builder.row_table, fields, tables, row_columns: Vec::new(), revs: Vec::new(), select: String::new(), select_lean: String::new() };
    for plan in &layout.fields {
        match plan {
            FieldPlan::Leaf { leaf, .. } => layout.row_columns.push((leaf.column.clone(), leaf.ty)),
            FieldPlan::Typed { member, .. } => member_columns(member, &mut layout.row_columns),
            FieldPlan::Rest { .. } => {}
        }
    }
    layout.revs = layout.fields.iter().map(|plan| (plan.field().to_string(), rev_column(&column_name(plan.field())))).collect();
    let mut names = vec!["id".to_string(), "revision".to_string(), "born_rev".to_string()];
    names.extend(layout.row_columns.iter().map(|(name, _)| name.clone()));
    if layout.has_rest() {
        names.push("rest".to_string());
    }
    layout.select_lean = names.join(", ");
    names.extend(layout.revs.iter().map(|(_, column)| column.clone()));
    layout.select = names.join(", ");
    layout
}

fn number(member: &mut Member, next: &mut usize) {
    match member {
        Member::Leaf(leaf) => {
            leaf.pos = *next;
            *next += 1;
        }
        Member::Object { at, members, .. } => {
            *at = *next;
            *next += 1;
            members.iter_mut().for_each(|child| number(child, next));
        }
        Member::List { at, .. } | Member::Array { at, .. } => {
            *at = *next;
            *next += 1;
        }
    }
}

pub fn member_present(member: &Member) -> &str {
    match member {
        Member::Object { present, .. } | Member::List { present, .. } | Member::Array { present, .. } => present,
        Member::Leaf(leaf) => &leaf.column,
    }
}

pub fn member_columns(member: &Member, out: &mut Vec<(String, ColumnType)>) {
    match member {
        Member::Leaf(leaf) => out.push((leaf.column.clone(), leaf.ty)),
        Member::Object { present, members, .. } => {
            out.push((present.clone(), ColumnType::Integer));
            members.iter().for_each(|child| member_columns(child, out));
        }
        Member::List { present, .. } | Member::Array { present, .. } => out.push((present.clone(), ColumnType::Integer)),
    }
}

impl Layout {
    pub fn row_columns(&self) -> &[(String, ColumnType)] {
        &self.row_columns
    }

    pub fn has_rest(&self) -> bool {
        self.fields.iter().any(|plan| matches!(plan, FieldPlan::Rest { .. }))
    }

    pub fn rev_columns(&self) -> &[(String, String)] {
        &self.revs
    }

    pub fn plan(&self, field: &str) -> Option<&FieldPlan> {
        self.fields.iter().find(|plan| plan.field() == field)
    }

    pub fn table_statements(&self, temp: bool) -> Vec<String> {
        let kind = if temp { "CREATE TEMP TABLE" } else { "CREATE TABLE" };
        self.tables
            .iter()
            .map(|table| match &table.shape {
                TableShape::Values(value) => format!(
                    "{kind} IF NOT EXISTS {} (\n  row_id TEXT NOT NULL,\n  owner INTEGER NOT NULL,\n  pos INTEGER NOT NULL,\n  value {},\n  PRIMARY KEY (row_id, owner, pos)\n) STRICT, WITHOUT ROWID",
                    table.name,
                    value.ty.as_sql()
                ),
                TableShape::Elements(members) => {
                    let mut columns = Vec::new();
                    members.iter().for_each(|member| member_columns(member, &mut columns));
                    let extra: String = columns.iter().map(|(name, ty)| format!(",\n  {name} {}", ty.as_sql())).collect();
                    format!(
                        "{kind} IF NOT EXISTS {} (\n  row_id TEXT NOT NULL,\n  node INTEGER NOT NULL,\n  owner INTEGER NOT NULL,\n  pos INTEGER NOT NULL{extra},\n  PRIMARY KEY (row_id, node)\n) STRICT, WITHOUT ROWID",
                        table.name
                    )
                }
            })
            .collect()
    }
}
