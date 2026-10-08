use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::{json, Value};

use crate::commit::ScopeRef;
use crate::identity::{ModelKey, RowId, RowRef};
use crate::query::{compile_count, fetch_count, Order, Query, Where};
use crate::schema::{AssociationKind, AssociationSpec, ModelSpec};
use crate::store::Reader;
use crate::value::{read_id, SortDirection};

use super::diff::{Diff, Entry, Stamp};
use super::dispatch::Event;
use super::error::DbError;
use super::open::Shared;
use super::row::Row;

pub type Related = BTreeMap<String, BTreeMap<RowId, Vec<Row>>>;

#[derive(Debug, Clone)]
pub struct Change {
    pub revision: i64,
    pub rows: Vec<Row>,
    pub related: Related,
    pub diff: Diff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CountChange {
    pub revision: i64,
    pub count: i64,
}

pub(crate) struct Step {
    pub from: i64,
    pub diff: Diff,
}

pub(crate) struct RowsBody {
    pub entries: Vec<Entry>,
    pub rows: Vec<Row>,
    pub related: Related,
    pub step: Option<Step>,
}

pub(crate) enum Body {
    Rows(RowsBody),
    Count(i64),
}

pub(crate) struct Version {
    pub revision: i64,
    pub body: Body,
}

impl Version {
    pub(crate) fn succeeding(self, previous: &Version, replaced: &[(RowId, RowId)]) -> Option<Version> {
        match (&previous.body, self.body) {
            (Body::Rows(old), Body::Rows(new)) => {
                let diff = Diff::between(&old.entries, &new.entries, replaced);
                (!diff.is_empty()).then(|| Version { revision: self.revision, body: Body::Rows(RowsBody { step: Some(Step { from: previous.revision, diff }), ..new }) })
            }
            (Body::Count(old), Body::Count(new)) => (*old != new).then_some(Version { revision: self.revision, body: Body::Count(new) }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Subject {
    Rows(Query),
    Row(RowRef),
    Count(Query),
}

impl Subject {
    pub(crate) fn key(&self) -> String {
        match self {
            Subject::Rows(query) => format!("rows:{}", serde_json::to_string(query).expect("query serializes")),
            Subject::Row(row) => format!("row:{}:{}", row.model, row.id),
            Subject::Count(query) => format!("count:{}", serde_json::to_string(query).expect("query serializes")),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Deps {
    pub models: BTreeSet<ModelKey>,
    pub scopes: BTreeSet<ScopeRef>,
    pub rows: BTreeSet<RowRef>,
    pub reset_models: BTreeSet<ModelKey>,
}

impl Deps {
    pub(crate) fn hit(&self, event: &Event) -> bool {
        event.reset_all
            || !self.reset_models.is_disjoint(&event.reset_models)
            || !self.models.is_disjoint(&event.models)
            || !self.scopes.is_disjoint(&event.scopes)
            || !self.rows.is_disjoint(&event.rows)
    }
}

fn association<'a>(spec: &'a ModelSpec, name: &str) -> Result<&'a AssociationSpec, DbError> {
    spec.associations
        .iter()
        .find(|association| association.name == name)
        .ok_or_else(|| DbError::UnknownAssociation { model: spec.key.to_string(), name: name.to_string() })
}

impl Shared {
    pub(crate) fn deps(&self, subject: &Subject) -> Result<Deps, DbError> {
        match subject {
            Subject::Row(row) => {
                self.spec(row.model.as_str())?;
                Ok(Deps { rows: BTreeSet::from([row.clone()]), reset_models: BTreeSet::from([row.model.clone()]), ..Deps::default() })
            }
            Subject::Rows(query) | Subject::Count(query) => {
                let spec = self.spec(query.model.as_str())?;
                let mut deps = Deps { reset_models: BTreeSet::from([query.model.clone()]), ..Deps::default() };
                match &query.scope {
                    Some(key) => {
                        deps.scopes.insert(ScopeRef::new(query.model.clone(), key.clone()));
                    }
                    None => {
                        deps.models.insert(query.model.clone());
                    }
                }
                if matches!(subject, Subject::Rows(_)) {
                    for name in &query.include {
                        let target = association(spec, name)?.target.clone();
                        deps.models.insert(target.clone());
                        deps.reset_models.insert(target);
                    }
                }
                Ok(deps)
            }
        }
    }

    pub(crate) fn evaluate(&self, subject: &Subject) -> Result<Version, DbError> {
        self.read(|reader| {
            let revision = reader.revision_seq()?;
            let body = match subject {
                Subject::Rows(query) => {
                    let keys = self.query_keys(reader, query)?;
                    let rows = self.rows_for_keys(reader, query.model.as_str(), &keys)?;
                    self.rows_body(reader, query, rows)?
                }
                Subject::Row(row) => {
                    let ids = [row.id.to_string()];
                    let rows = self.load(reader, row.model.as_str(), &ids)?.into_values().collect();
                    self.rows_body(reader, &Query::new(row.model.as_str()), rows)?
                }
                Subject::Count(query) => {
                    let statement = compile_count(query, self.spec(query.model.as_str())?)?;
                    Body::Count(fetch_count(reader.connection(), &statement)?)
                }
            };
            Ok(Version { revision, body })
        })
    }

    fn rows_body(&self, reader: &Reader<'_>, query: &Query, rows: Vec<Row>) -> Result<Body, DbError> {
        let spec = self.spec(query.model.as_str())?;
        let (related, mut stamps) = self.related_rows(reader, spec, &query.include, &rows)?;
        let entries = rows
            .iter()
            .map(|row| Entry { id: row.id.clone(), stamp: Stamp { revision: row.revision, related: stamps.remove(&row.id).unwrap_or_default() } })
            .collect();
        Ok(Body::Rows(RowsBody { entries, rows, related, step: None }))
    }

    fn related_rows(
        &self,
        reader: &Reader<'_>,
        spec: &ModelSpec,
        include: &[String],
        rows: &[Row],
    ) -> Result<(Related, HashMap<RowId, Vec<(RowRef, i64)>>), DbError> {
        let mut related = Related::new();
        let mut stamps: HashMap<RowId, Vec<(RowRef, i64)>> = HashMap::new();
        for name in include {
            let association = association(spec, name)?;
            let target = association.target.as_str();
            let per_parent = match association.kind {
                AssociationKind::BelongsTo => {
                    let wanted: Vec<(RowId, Vec<RowId>)> =
                        rows.iter().map(|row| (row.id.clone(), row.get(&association.foreign_key).and_then(read_id).into_iter().collect())).collect();
                    self.linked(reader, target, wanted)?
                }
                AssociationKind::References => {
                    let field = if association.foreign_key.is_empty() { &association.name } else { &association.foreign_key };
                    let wanted: Vec<(RowId, Vec<RowId>)> =
                        rows.iter().map(|row| (row.id.clone(), row.get(field).and_then(Value::as_array).into_iter().flatten().filter_map(read_id).collect())).collect();
                    self.linked(reader, target, wanted)?
                }
                AssociationKind::HasOne | AssociationKind::HasMany => {
                    let parents: Vec<Value> = rows.iter().map(|row| json!(row.id.as_str())).collect();
                    let query = Query::new(target)
                        .filtered(Where::in_list(&association.foreign_key, parents))
                        .ordered(vec![Order::new("id", SortDirection::Asc)]);
                    let keys = self.query_keys(reader, &query)?;
                    let children = self.rows_for_keys(reader, target, &keys)?;
                    let mut by_parent: HashMap<String, Vec<Row>> = HashMap::new();
                    for child in children {
                        if let Some(parent) = child.get(&association.foreign_key).and_then(read_id) {
                            by_parent.entry(parent.to_string()).or_default().push(child);
                        }
                    }
                    let single = association.kind == AssociationKind::HasOne;
                    rows.iter()
                        .map(|row| {
                            let mut found = by_parent.remove(row.id.as_str()).unwrap_or_default();
                            if single {
                                found.truncate(1);
                            }
                            (row.id.clone(), found)
                        })
                        .collect()
                }
            };
            let slot = related.entry(name.clone()).or_default();
            for (parent, found) in per_parent {
                if found.is_empty() {
                    continue;
                }
                let stamp = stamps.entry(parent.clone()).or_default();
                stamp.extend(found.iter().map(|row| (RowRef::new(association.target.clone(), row.id.clone()), row.revision)));
                slot.insert(parent, found);
            }
        }
        Ok((related, stamps))
    }

    fn linked(&self, reader: &Reader<'_>, target: &str, wanted: Vec<(RowId, Vec<RowId>)>) -> Result<Vec<(RowId, Vec<Row>)>, DbError> {
        let unique: BTreeSet<String> = wanted.iter().flat_map(|(_, ids)| ids.iter().map(RowId::to_string)).collect();
        let ids: Vec<String> = unique.into_iter().collect();
        let loaded = self.load(reader, target, &ids)?;
        Ok(wanted.into_iter().map(|(parent, ids)| (parent, ids.iter().filter_map(|id| loaded.get(id.as_str()).cloned()).collect())).collect())
    }
}
