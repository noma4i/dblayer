//! One-shot reads of the local database: the rows of a query or one row, without a stream and without the server.

use super::wire::WireRow;
use super::{Database, DbError};

/// The rows of a query that were read without a stream.
#[derive(Clone, Debug, uniffi::Record)]
pub struct CachedQuery {
    pub revision: i64,
    pub rows: Vec<WireRow>,
}

#[uniffi::export]
impl Database {
    /// The rows of a query the core already holds, without a stream and without reading SQLite on the calling thread.
    pub async fn peek_query(&self, epoch: u64, query_json: String) -> Result<Option<CachedQuery>, DbError> {
        self.read_in_epoch(epoch, "peek query", move |this| {
            let (query, _) = this.parsed_query(&query_json)?;
            let wire = this.wire(query.model.as_str())?;
            Ok(this.db.peek_query(&query).map(|peeked| CachedQuery { revision: peeked.revision, rows: peeked.rows.iter().map(|row| wire.row(row)).collect() }))
        }).await
    }

    /// The rows of a query read from SQLite once, off the calling thread. Nothing is loaded from the server and no stream stays open.
    pub async fn read_query(&self, epoch: u64, query_json: String) -> Result<Vec<WireRow>, DbError> {
        self.read_in_epoch(epoch, "read query", move |this| {
            let (query, _) = this.parsed_query(&query_json)?;
            let wire = this.wire(query.model.as_str())?;
            Ok(this.db.query(&query)?.iter().map(|row| wire.row(row)).collect())
        }).await
    }

    /// One row read from SQLite once, off the calling thread.
    pub async fn read_row(&self, epoch: u64, model: String, row_id: String) -> Result<Option<WireRow>, DbError> {
        self.read_in_epoch(epoch, "read row", move |this| {
            let wire = this.wire(&model)?;
            Ok(this.db.find(&model, &super::database::id(&row_id)?)?.map(|row| wire.row(&row)))
        }).await
    }
}

