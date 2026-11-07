//! The routing of `Committed` to the streams as code: delivery.md rows dlv_opening_committed and dlv_live_committed with D-84 and P-DLV-27. No library code.

use std::collections::{BTreeMap, BTreeSet};

pub(crate) type StreamId = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Watched {
    /// A `List` or `Count`: every row of its tables.
    Tables,
    /// An `Entity`: the row `(table, id)`, the other tables of its view are link targets.
    Row { table: String, id: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Stream {
    pub(crate) tables: Vec<String>,
    pub(crate) watched: Watched,
}

/// A write as the Store applied it, by model.
#[derive(Clone, Debug, Default)]
pub(crate) struct Write {
    pub(crate) rows: Vec<(String, String)>,
    pub(crate) scopes: Vec<String>,
    pub(crate) members: Vec<(String, String)>,
    pub(crate) renewed: Vec<(String, String)>,
    pub(crate) replaced: Vec<((String, String), String)>,
    pub(crate) queue: bool,
}

/// What the broadcast says: every stream, or the tables and the rows of the write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Notice {
    All,
    Of { tables: BTreeSet<String>, rows: Option<BTreeSet<(String, String)>> },
}

pub(crate) fn table_of(model: &str) -> String { format!("row_{model}") }

/// store.md P-STORE-28: `tables` = the tables of the rows, scopes and members written (and the queue), `rows` = exactly the rows written in those tables.
pub(crate) fn notice_of_write(write: &Write) -> Notice {
    let mut tables: BTreeSet<String> = write.rows.iter().map(|(model, _)| table_of(model)).chain(write.scopes.iter().map(|model| table_of(model))).chain(write.members.iter().map(|(model, _)| table_of(model))).chain(write.replaced.iter().map(|((model, _), _)| table_of(model))).collect();
    if write.queue { tables.insert("op".into()); }
    let mut rows: BTreeSet<(String, String)> = write.rows.iter().chain(write.members.iter()).chain(write.renewed.iter()).map(|(model, id)| (table_of(model), id.clone())).collect();
    for ((model, from), to) in &write.replaced {
        rows.insert((table_of(model), from.clone()));
        rows.insert((table_of(model), to.clone()));
    }
    rows.retain(|(table, _)| tables.contains(table));
    Notice::Of { tables, rows: Some(rows) }
}

pub(crate) fn notice_of_reset(models: Option<&[&str]>) -> Notice {
    match models {
        None => Notice::All,
        Some(models) => Notice::Of { tables: models.iter().map(|model| table_of(model)).collect(), rows: None },
    }
}

/// The index of the streams: by table for rows kinds and link targets, by `(table, row)` for the own row of an Entity.
#[derive(Default)]
pub(crate) struct Fanout {
    by_table: BTreeMap<String, BTreeSet<StreamId>>,
    by_row: BTreeMap<(String, String), BTreeSet<StreamId>>,
}

impl Fanout {
    pub(crate) fn of(streams: &BTreeMap<StreamId, Stream>) -> Self {
        let mut index = Self::default();
        for (id, stream) in streams {
            for table in &stream.tables {
                match &stream.watched {
                    Watched::Row { table: own, id: row } if own == table => { index.by_row.entry((table.clone(), row.clone())).or_default().insert(*id); }
                    _ => { index.by_table.entry(table.clone()).or_default().insert(*id); }
                }
            }
        }
        index
    }

    pub(crate) fn affected(&self, notice: &Notice) -> BTreeSet<StreamId> {
        match notice {
            Notice::All => self.by_table.values().chain(self.by_row.values()).flatten().copied().collect(),
            Notice::Of { tables, rows } => {
                let tabled = tables.iter().filter_map(|table| self.by_table.get(table)).flatten();
                let rowed = self.by_row.iter().filter(|((table, id), _)| tables.contains(table) && rows.as_ref().is_none_or(|rows| rows.contains(&(table.clone(), id.clone())))).flat_map(|(_, streams)| streams);
                tabled.chain(rowed).copied().collect()
            }
        }
    }
}
