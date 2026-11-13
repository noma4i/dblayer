//! One-shot reads of the local database: the rows of a query or one row, without a stream and without the server.

use super::wire::WireRow;
use super::{Database, DbError, QuerySpec};

#[uniffi::export]
impl Database {
    /// The rows of a query read from SQLite once, off the calling thread. Nothing is loaded from the server and no stream stays open.
    pub async fn read_query(&self, epoch: u64, query: QuerySpec) -> Result<Vec<WireRow>, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.read_in_epoch(epoch, "read query", move |this| {
            let (query, _) = this.parsed_query(&query)?;
            let wire = this.wire(query.model.as_str())?;
            Ok(this.db.query(&query)?.iter().map(|row| wire.row(row)).collect())
        }).await
    }).await? }

    /// One row read from SQLite once, off the calling thread.
    pub async fn read_row(&self, epoch: u64, model: String, row_id: String) -> Result<Option<WireRow>, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.account_call(epoch)?;
        this_db.wire(&model)?;
        let row_id = this_db.resolved(epoch, &model, super::database::id(&row_id)?).await?;
        this_db.read_in_epoch(epoch, "read row", move |this| {
            let wire = this.wire(&model)?;
            Ok(this.db.find(&model, &row_id)?.map(|row| wire.row(&row)))
        }).await
    }).await? }
}

