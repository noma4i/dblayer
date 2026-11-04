use crate::lifecycle::{Authority, Boundary, End, Refused};

use super::error::DbError;
use super::open::Db;

impl Db {
    pub(crate) async fn end_session<E>(&self, end: End, expected: Option<u64>, clear: impl FnOnce(&Authority) -> Result<(), E> + Send + 'static) -> Result<(), E>
    where
        E: From<DbError> + From<Refused> + Send + 'static,
    {
        let db = self.clone();
        crate::task::spawn(crate::task::Carry::Detached, async move { db.end_session_owned(end, expected, clear).await })
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?
    }

    /// The transition runs on its own task: a caller whose lease the transition ended is dropped at its next poll, and the transition must still finish.
    async fn end_session_owned<E>(&self, end: End, expected: Option<u64>, clear: impl FnOnce(&Authority) -> Result<(), E> + Send + 'static) -> Result<(), E>
    where
        E: From<DbError> + From<Refused> + Send + 'static,
    {
        let permit = match self.shared.store.lifecycle.begin(Boundary::SessionEnd(end), expected).await {
            Ok(permit) => permit,
            Err(Refused::Joined) => return Ok(()),
            Err(refused) => return Err(refused.into()),
        };
        let db = self.clone();
        let permit = crate::task::spawn_blocking(crate::task::Carry::Detached, move || -> Result<_, E> {
            permit.run_on_this_thread();
            db.shared.store.write_session_end_intent(&permit).map_err(DbError::from)?;
            db.shared.store.end_session(&permit);
            Ok(permit)
        })
        .await
        .map_err(|error| DbError::Internal(error.to_string()))??;
        let db = self.clone();
        crate::task::spawn_blocking(crate::task::Carry::Detached, move || -> Result<(), E> {
            permit.run_on_this_thread();
            clear(&permit.authority())?;
            db.reset_all_in(&permit)?;
            db.await_revision(db.revision_seq()?);
            permit.finish();
            Ok(())
        })
        .await
        .map_err(|error| DbError::Internal(error.to_string()))?
    }
}
