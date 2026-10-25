use serde_json::{Map, Value};

use crate::read::{LoadMoreVerdict, PageChain};
use crate::schema::RemoteShape;

use super::error::SyncError;
use super::load::{DiscardReason, LoadOptions, LoadStatus, Resolved};
use super::{Inner, Sync};

impl Inner {
    fn incomplete(resolved: &Resolved<'_>, reason: &str) -> SyncError {
        SyncError::Incomplete { model: resolved.model.clone(), relation: resolved.relation.clone(), reason: reason.to_string() }
    }

    async fn load_all(&self, resolved: &Resolved<'_>, force: bool) -> Result<(), SyncError> {
        let first = self.fetch(resolved, None, force).await?;
        if let Some(error) = first.kept_error {
            return Err(error);
        }
        let RemoteShape::Connection { max_pages, .. } = resolved.remote.shape else { return Ok(()) };
        if first.status == LoadStatus::Discarded(DiscardReason::StaleGeneration) {
            return Err(Self::incomplete(resolved, "the database was reset"));
        }
        loop {
            self.flights_settled(resolved).await;
            let Some(window) = self.db.shared.store.window(&resolved.window_key)? else { return Err(Self::incomplete(resolved, "the window is gone")) };
            let chain = PageChain { cursor: window.cursor.clone(), has_next: window.has_next, loaded_pages: usize::try_from(window.pages).unwrap_or_default(), is_loading: false };
            match LoadMoreVerdict::of(&chain, max_pages.map(|pages| pages as usize)) {
                LoadMoreVerdict::NoCursor if window.has_next => return Err(Self::incomplete(resolved, "the server reports a next page without a cursor")),
                LoadMoreVerdict::NoCursor | LoadMoreVerdict::AlreadyLoading => return Ok(()),
                LoadMoreVerdict::MaxPagesReached => return Err(Self::incomplete(resolved, "max_pages is reached and more pages remain")),
                LoadMoreVerdict::FetchNextPage => {}
            }
            let outcome = self.fetch(resolved, window.cursor.clone(), true).await?;
            if let Some(error) = outcome.kept_error {
                return Err(error);
            }
            match outcome.status {
                LoadStatus::Discarded(DiscardReason::StaleGeneration) => return Err(Self::incomplete(resolved, "the database was reset")),
                LoadStatus::Discarded(_) => continue,
                _ => {}
            }
            let after = self.db.shared.store.window(&resolved.window_key)?;
            if after.is_some_and(|after| after.has_next && after.cursor == window.cursor) {
                return Err(Self::incomplete(resolved, "the cursor did not advance"));
            }
        }
    }
}

impl Sync {
    pub async fn load_all(&self, model: &str, relation: &str, params: &Map<String, Value>, options: LoadOptions) -> Result<(), SyncError> {
        let resolved = self.inner.resolve(model, relation, params)?;
        self.inner.load_all(&resolved, options.force).await
    }
}
