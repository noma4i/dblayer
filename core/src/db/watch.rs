use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use serde_json::{json, Value};

use crate::commit::ScopeRef;
use crate::identity::{ModelKey, RowId, RowRef};
use crate::identity::touches::touch;
use crate::query::{compile_count, compile_ranked, fetch_count, fetch_ranked, ranking, Order, Query, RankedRow, Where};
use crate::schema::{AssociationKind, AssociationSpec, ModelSpec};
use crate::store::Reader;
use crate::value::{read_id, SortDirection};

use super::diff::{Diff, Entry, Stamp};
use super::lookup::RowKey;
use super::ranked::{OrderKey, Ranked};
use super::dispatch::Event;
use super::error::DbError;
use super::open::{locked, Shared};
use super::row::Row;
use super::window::{WindowEdge, WindowPart, WindowSeed, WindowSpec};

const MAX_STEP_IDS: usize = 500;

pub type Related = BTreeMap<String, BTreeMap<RowId, Vec<Row>>>;
type ParentStamps = HashMap<RowId, Vec<(RowRef, Stamp)>>;

#[derive(Debug, Clone)]
pub struct Change {
    pub revision: i64,
    pub rows: Vec<Row>,
    pub related: Related,
    pub diff: Diff,
    pub full: bool,
    pub size: usize,
    pub window: Option<WindowEdge>,
}

impl Change {
    pub fn apply_to(&self, rows: &mut Vec<Row>) {
        if self.full {
            rows.clone_from(&self.rows);
            return;
        }
        let supplied: HashMap<&RowId, &Row> = self.rows.iter().map(|row| (&row.id, row)).collect();
        for (_, to) in &self.diff.replaced {
            if let (Some(at), Some(row)) = (self.diff.was.get(to), supplied.get(to)) {
                rows[*at] = (*row).clone();
            }
        }
        let removed = self.diff.removed.iter().map(|id| (id, false));
        let moved = self.diff.moved.iter().map(|id| (id, true));
        let mut leaving: Vec<(usize, &RowId, bool)> = removed.chain(moved).filter_map(|(id, kept)| self.diff.was.get(id).map(|at| (*at, id, kept))).collect();
        leaving.sort_unstable_by(|left, right| right.0.cmp(&left.0));
        let mut taken: HashMap<&RowId, Row> = HashMap::new();
        for (at, id, kept) in leaving {
            let row = rows.remove(at);
            if kept {
                taken.insert(id, row);
            }
        }
        for (id, index) in self.diff.placed() {
            let row = taken.remove(&id).or_else(|| supplied.get(&id).map(|row| (*row).clone())).expect("a placed row is known");
            rows.insert(index, row);
        }
        for row in &self.rows {
            rows[self.diff.at[&row.id]] = row.clone();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CountChange {
    pub revision: i64,
    pub count: i64,
}

pub(crate) struct Step {
    pub from: (i64, u64),
    pub diff: Diff,
    pub rows: Vec<Row>,
    pub related: Related,
    pub touched: Option<HashSet<String>>,
}

impl Step {
    pub(crate) fn supply(&self) -> Loaded {
        Loaded { rows: self.rows.iter().map(|row| (row.id.to_string(), row.clone())).collect(), related: self.related.clone() }
    }
}

pub(crate) struct ById(Arc<Entry>);

impl PartialEq for ById {
    fn eq(&self, other: &Self) -> bool {
        self.0.id.as_str() == other.0.id.as_str()
    }
}

impl Eq for ById {}

impl std::hash::Hash for ById {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.id.as_str().hash(state);
    }
}

impl std::borrow::Borrow<str> for ById {
    fn borrow(&self) -> &str {
        self.0.id.as_str()
    }
}

pub(crate) struct Shown {
    pub entries: Vec<Arc<Entry>>,
    pub index: HashSet<ById>,
}

impl Shown {
    pub(crate) fn of(entries: Vec<Arc<Entry>>) -> Arc<Self> {
        let index = entries.iter().cloned().map(ById).collect();
        Arc::new(Self { entries, index })
    }
}

pub(crate) enum Listing {
    Whole(Arc<Shown>),
    Ranked(Ranked),
}

pub(crate) struct RowsBody {
    pub list: Listing,
    pub changed: Option<HashSet<String>>,
    pub step: Option<Step>,
    pub window: Option<WindowPart>,
}

impl RowsBody {
    pub(crate) fn new(entries: Vec<Arc<Entry>>) -> Self {
        Self { list: Listing::Whole(Shown::of(entries)), changed: None, step: None, window: None }
    }

    pub(crate) fn ranked(list: Ranked, changed: Option<HashSet<String>>) -> Self {
        Self { list: Listing::Ranked(list), changed, step: None, window: None }
    }

    pub(crate) fn same_rows(&self, other: &Self) -> bool {
        matches!((&self.list, &other.list), (Listing::Whole(left), Listing::Whole(right)) if Arc::ptr_eq(left, right))
    }

    pub(crate) fn ranking(&self) -> Option<&Ranked> {
        match &self.list {
            Listing::Ranked(list) => Some(list),
            Listing::Whole(_) => None,
        }
    }

    pub(crate) fn entries(&self) -> Cow<'_, [Arc<Entry>]> {
        match &self.list {
            Listing::Whole(shown) => Cow::Borrowed(&shown.entries),
            Listing::Ranked(list) => Cow::Owned(list.entries()),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.list {
            Listing::Whole(shown) => shown.entries.len(),
            Listing::Ranked(list) => list.len(),
        }
    }

    pub(crate) fn entry(&self, id: &str) -> Option<Arc<Entry>> {
        match &self.list {
            Listing::Whole(shown) => shown.index.get(id).map(|found| found.0.clone()),
            Listing::Ranked(list) => list.get(id).map(|(_, entry)| entry.clone()),
        }
    }
}

pub(crate) enum Body {
    Rows(RowsBody),
    Count(i64),
}

#[derive(Default, Clone)]
pub(crate) struct Loaded {
    pub rows: HashMap<String, Row>,
    pub related: Related,
}

pub(crate) struct Evaluated {
    pub version: Version,
    pub loaded: Loaded,
}

pub(crate) struct Version {
    pub revision: i64,
    pub epoch: u64,
    pub body: Body,
}

impl Version {
    pub(crate) fn position(&self) -> (i64, u64) {
        (self.revision, self.epoch)
    }

    pub(crate) fn succeeding(self, previous: &Version, replaced: &[(RowId, RowId)], mut loaded: Loaded) -> Option<Version> {
        match (&previous.body, self.body) {
            (Body::Rows(old), Body::Rows(new)) => {
                let diff = match (old.ranking(), new.ranking(), &new.changed) {
                    (Some(before), Some(after), Some(changed)) => Diff::stepping(before, after, changed, replaced),
                    _ if old.same_rows(&new) => Diff::default(),
                    _ => Diff::between(&old.entries()[..], &new.entries()[..], replaced),
                };
                if diff.is_empty() && new.window.is_none() {
                    return None;
                }
                let (rows, related) = changed_rows(&diff, &mut loaded.rows, &mut loaded.related);
                let step = Step { from: previous.position(), diff, rows, related, touched: new.changed.clone() };
                Some(Version { revision: self.revision, epoch: self.epoch, body: Body::Rows(RowsBody { step: Some(step), ..new }) })
            }
            (Body::Count(old), Body::Count(new)) => (*old != new).then_some(Version { revision: self.revision, epoch: self.epoch, body: Body::Count(new) }),
            _ => None,
        }
    }
}

fn ranked_entry(record: RankedRow, row: &Row, descending: &[bool]) -> (Arc<OrderKey>, Arc<Entry>) {
    let key = Arc::new(OrderKey::new(record.cells, descending, record.id));
    (key, Arc::new(Entry { id: row.id.clone(), stamp: Stamp::of(row, Vec::new()) }))
}

pub(crate) fn changed_rows(diff: &Diff, rows: &mut HashMap<String, Row>, related: &mut Related) -> (Vec<Row>, Related) {
    let replaced_to = diff.replaced.iter().map(|(_, to)| to);
    let mut ids: Vec<&RowId> = diff.inserted.iter().chain(&diff.updated).chain(replaced_to).collect();
    ids.sort_by_key(|id| diff.at.get(*id).copied().unwrap_or(usize::MAX));
    let picked: Vec<Row> = ids.iter().filter_map(|id| rows.remove(id.as_str())).collect();
    (picked, related_of(&ids, related))
}

pub(crate) fn related_of(parents: &[&RowId], related: &Related) -> Related {
    let wanted: HashSet<&RowId> = parents.iter().copied().collect();
    related
        .iter()
        .map(|(name, per_parent)| (name.clone(), per_parent.iter().filter(|(parent, _)| wanted.contains(parent)).map(|(parent, found)| (parent.clone(), found.clone())).collect::<BTreeMap<_, _>>()))
        .filter(|(_, per_parent)| !per_parent.is_empty())
        .collect()
}

#[derive(Debug, Clone)]
pub(crate) enum Subject {
    Rows(Query),
    Row(RowRef),
    Count(Query),
    Window(Query, WindowSpec, u64),
}

impl Subject {
    pub(crate) fn key(&self) -> String {
        match self {
            Subject::Rows(query) => format!("rows:{}", serde_json::to_string(query).expect("query serializes")),
            Subject::Row(row) => format!("row:{}:{}", row.model, row.id),
            Subject::Count(query) => format!("count:{}", serde_json::to_string(query).expect("query serializes")),
            Subject::Window(query, _, nonce) => format!("window:{nonce}:{}", serde_json::to_string(query).expect("query serializes")),
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
    pub(crate) fn keyed(&self) -> bool {
        self.models.is_empty() && self.scopes.is_empty() && !self.rows.is_empty()
    }

    pub(crate) fn hit(&self, event: &Event) -> bool {
        event.reset_all
            || !self.reset_models.is_disjoint(&event.reset_models)
            || !self.models.is_disjoint(&event.models)
            || !self.scopes.is_disjoint(&event.scopes)
            || !self.rows.is_disjoint(&event.rows)
    }
}

fn pinned_ids(query: &Query) -> Option<Vec<RowId>> {
    if query.scope.is_some() || !query.include.is_empty() {
        return None;
    }
    fn of(clause: &Where) -> Option<Vec<RowId>> {
        match clause {
            Where::Equal { field, value } if field == "id" => RowId::new(value.as_str()?).ok().map(|id| vec![id]),
            Where::InList { field, values } if field == "id" => values.iter().map(|value| RowId::new(value.as_str()?).ok()).collect(),
            Where::AllOf { clauses } => clauses.iter().find_map(of),
            _ => None,
        }
    }
    query.r#where.as_ref().and_then(of).filter(|ids| !ids.is_empty())
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
            Subject::Rows(query) | Subject::Count(query) | Subject::Window(query, _, _) => {
                let spec = self.spec(query.model.as_str())?;
                let mut deps = Deps { reset_models: BTreeSet::from([query.model.clone()]), ..Deps::default() };
                if let (Subject::Rows(_), Some(ids)) = (subject, pinned_ids(query)) {
                    deps.rows = ids.into_iter().map(|id| RowRef::new(query.model.clone(), id)).collect();
                    return Ok(deps);
                }
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

    pub(crate) fn evaluate(&self, subject: &Subject) -> Result<Evaluated, DbError> {
        self.evaluate_seeded(subject, None)
    }

    pub(crate) fn evaluate_seeded(&self, subject: &Subject, seed: Option<&WindowSeed>) -> Result<Evaluated, DbError> {
        let epoch = self.ephemeral.lock().epoch;
        let models = match subject {
            Subject::Rows(query) => self.query_models(query.model.as_str(), &query.include)?,
            Subject::Count(query) | Subject::Window(query, _, _) => vec![query.model.as_str()],
            Subject::Row(row) => vec![row.model.as_str()],
        };
        self.read(&models, |reader| {
            let revision = reader.revision_seq()?;
            let (body, loaded) = match subject {
                Subject::Rows(query) if ranking(query) => self.ranked_body(reader, query)?,
                Subject::Window(query, spec, _) => self.window_body(reader, query, spec, seed.filter(|seed| seed.at == (revision, epoch)))?,
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
                    (Body::Count(fetch_count(reader.connection(), &statement)?), Loaded::default())
                }
            };
            Ok(Evaluated { version: Version { revision, epoch, body }, loaded })
        })
    }

    pub(crate) fn evaluate_step(&self, query: &Query, previous: &Version, touched: &HashSet<String>, horizon: (i64, u64)) -> Result<Evaluated, DbError> {
        let Body::Rows(old) = &previous.body else { return self.evaluate(&Subject::Rows(query.clone())) };
        if ranking(query) {
            let stepped = match old.ranking() {
                Some(ranked) => self.rank_step(query, previous, ranked, touched, horizon)?,
                None => None,
            };
            return match stepped {
                Some(evaluated) => Ok(evaluated),
                None => self.evaluate(&Subject::Rows(query.clone())),
            };
        }
        let epoch = self.ephemeral.lock().epoch;
        self.read(&[query.model.as_str()], |reader| {
            let revision = reader.revision_seq()?;
            let keys = self.query_keys(reader, query)?;
            let mut wanted = Vec::new();
            let slots: Vec<Option<Arc<Entry>>> = keys
                .iter()
                .map(|(id, key_revision)| match old.entry(id.as_str()) {
                    Some(entry) if (revision, epoch) == horizon && entry.stamp.revision == *key_revision && !touched.contains(id) => Some(entry),
                    _ => {
                        wanted.push((id.clone(), *key_revision));
                        None
                    }
                })
                .collect();
            let rows = self.rows_for_keys(reader, query.model.as_str(), &wanted)?;
            locked(&self.counters).rows_loaded += rows.len();
            let loaded: HashMap<String, Row> = rows.into_iter().map(|row| (row.id.to_string(), row)).collect();
            let entries = slots
                .into_iter()
                .zip(&keys)
                .filter_map(|(slot, (id, _))| slot.or_else(|| loaded.get(id).map(|row| Arc::new(Entry { id: row.id.clone(), stamp: Stamp::of(row, Vec::new()) }))))
                .collect();
            Ok(Evaluated { version: Version { revision, epoch, body: Body::Rows(RowsBody::new(entries)) }, loaded: Loaded { rows: loaded, related: Related::new() } })
        })
    }

    fn rank_step(&self, query: &Query, previous: &Version, old: &Ranked, touched: &HashSet<String>, horizon: (i64, u64)) -> Result<Option<Evaluated>, DbError> {
        let model = query.model.as_str();
        let spec = self.spec(model)?;
        let epoch = self.ephemeral.lock().epoch;
        self.read(&[model], |reader| {
            let revision = reader.revision_seq()?;
            if (revision, epoch) != horizon {
                return Ok(None);
            }
            let mut changed = touched.clone();
            changed.extend(self.dispatch.replaced_between(previous.revision, revision).iter().flat_map(|(from, to)| [from.to_string(), to.to_string()]));
            let wanted: Vec<String> = changed.iter().cloned().collect();
            let (mut found, mut descending) = (Vec::with_capacity(wanted.len()), Vec::new());
            for chunk in wanted.chunks(MAX_STEP_IDS) {
                let statement = compile_ranked(query, spec, Some(chunk))?;
                found.extend(fetch_ranked(reader.connection(), &statement.statement)?);
                descending = statement.descending;
            }
            let keys: Vec<RowKey> = found.iter().map(|record| (record.id.clone(), record.revision)).collect();
            let rows = self.rows_for_keys(reader, model, &keys)?;
            locked(&self.counters).rows_loaded += rows.len();
            let mut records: HashMap<String, RankedRow> = found.into_iter().map(|record| (record.id.clone(), record)).collect();
            let mut next = old.clone();
            for row in &rows {
                if let Some(record) = records.remove(row.id.as_str()) {
                    let (key, entry) = ranked_entry(record, row, &descending);
                    next = next.with(key, entry);
                }
            }
            let loaded = Loaded { rows: rows.into_iter().map(|row| (row.id.to_string(), row)).collect(), related: Related::new() };
            for id in changed.iter().filter(|id| !loaded.rows.contains_key(*id)) {
                next = next.without(id);
            }
            Ok(Some(Evaluated { version: Version { revision, epoch, body: Body::Rows(RowsBody::ranked(next, Some(changed))) }, loaded }))
        })
    }

    fn ranked_body(&self, reader: &Reader<'_>, query: &Query) -> Result<(Body, Loaded), DbError> {
        let statement = compile_ranked(query, self.spec(query.model.as_str())?, None)?;
        let found = fetch_ranked(reader.connection(), &statement.statement)?;
        touch(found.len());
        let keys: Vec<RowKey> = found.iter().map(|record| (record.id.clone(), record.revision)).collect();
        let rows = self.rows_for_keys(reader, query.model.as_str(), &keys)?;
        locked(&self.counters).rows_loaded += rows.len();
        let mut records = found.into_iter();
        let sorted = rows.iter().filter_map(|row| records.find(|record| record.id == row.id.as_str()).map(|record| ranked_entry(record, row, &statement.descending))).collect();
        let loaded = Loaded { rows: rows.into_iter().map(|row| (row.id.to_string(), row)).collect(), related: Related::new() };
        Ok((Body::Rows(RowsBody::ranked(Ranked::from_sorted(sorted), None)), loaded))
    }

    pub(crate) fn load_whole(&self, subject: &Subject, version: &Version) -> Result<Loaded, DbError> {
        let Body::Rows(body) = &version.body else { return Ok(Loaded::default()) };
        let (model, include) = match subject {
            Subject::Rows(query) => (query.model.clone(), query.include.clone()),
            Subject::Row(row) => (row.model.clone(), Vec::new()),
            Subject::Count(_) | Subject::Window(..) => return Ok(Loaded::default()),
        };
        let keys: Vec<(String, i64)> = body.entries().iter().map(|entry| (entry.id.to_string(), entry.stamp.revision)).collect();
        let models = self.query_models(model.as_str(), &include)?;
        self.read(&models, |reader| {
            let rows = self.rows_for_keys(reader, model.as_str(), &keys)?;
            locked(&self.counters).rows_loaded += rows.len();
            let related = self.related_rows(reader, self.spec(model.as_str())?, &include, &rows)?.0;
            Ok(Loaded { rows: rows.into_iter().map(|row| (row.id.to_string(), row)).collect(), related })
        })
    }

    fn query_models<'a>(&'a self, model: &'a str, include: &[String]) -> Result<Vec<&'a str>, DbError> {
        let spec = self.spec(model)?;
        let mut models = vec![model];
        for name in include {
            models.push(association(spec, name)?.target.as_str());
        }
        Ok(models)
    }

    fn rows_body(&self, reader: &Reader<'_>, query: &Query, rows: Vec<Row>) -> Result<(Body, Loaded), DbError> {
        let spec = self.spec(query.model.as_str())?;
        let (related, mut stamps) = self.related_rows(reader, spec, &query.include, &rows)?;
        locked(&self.counters).rows_loaded += rows.len();
        let entries = rows
            .iter()
            .map(|row| Arc::new(Entry { id: row.id.clone(), stamp: Stamp::of(row, stamps.remove(&row.id).unwrap_or_default()) }))
            .collect();
        let loaded = Loaded { rows: rows.into_iter().map(|row| (row.id.to_string(), row)).collect(), related };
        Ok((Body::Rows(RowsBody::new(entries)), loaded))
    }

    fn related_rows(
        &self,
        reader: &Reader<'_>,
        spec: &ModelSpec,
        include: &[String],
        rows: &[Row],
    ) -> Result<(Related, ParentStamps), DbError> {
        let mut related = Related::new();
        let mut stamps = ParentStamps::new();
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
                stamp.extend(found.iter().map(|row| (RowRef::new(association.target.clone(), row.id.clone()), Stamp::of(row, Vec::new()))));
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
