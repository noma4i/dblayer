use serde_json::{Map, Value};
use std::collections::HashSet;
use std::sync::Arc;

use crate::read::{LoadMoreVerdict, PageChain};
use crate::schema::RemoteShape;

use super::error::SyncError;
use super::load::{DiscardReason, LoadOptions, LoadStatus, Resolved};
use super::{Inner, Sync};
use super::load_machine::LoadError;
use super::load_owner::FlightTicket;

impl Inner {
    pub(crate) async fn load_members(self: &Arc<Self>, resolved: &Resolved<'_>, force: bool) -> Result<(), SyncError> {
        let origin = resolved.origin;
        if !self.admitted(origin) { return Err(SyncError::Cancelled); }
        let (inner, model, relation, params) = (self.clone(), resolved.model.clone(), resolved.relation.clone(), resolved.params.clone());
        let mut reader = self.owner.members(&resolved.window_key, |result| {
            crate::task::spawn(self.until_closed(async move {
                let loaded = match inner.resolve(origin, &model, &relation, &params) {
                    Ok(resolved) => inner.members_chain(&resolved, force).await,
                    Err(error) => Err(error),
                };
                crate::db::lock::effect(|| result.send_replace(Some(loaded)));
            })).abort_handle()
        });
        reader.finished().await
    }

    pub(crate) async fn members_chain(&self, resolved: &Resolved<'_>, force: bool) -> Result<(), SyncError> {
        let ticket = FlightTicket::new(&self.owner, &resolved.window_key, resolved.target(), false, self.stored(resolved)?);
        let result = match self.load_all(resolved, force).await { Ok(()) => self.stored(resolved), Err(error) => Err(error) };
        match &result {
            Ok(seed) => ticket.landed(seed.clone()),
            Err(error) => ticket.failed(LoadError::of(error)),
        }
        result.map(|_| ())
    }

    fn incomplete(resolved: &Resolved<'_>, reason: &str) -> SyncError {
        SyncError::Incomplete { model: resolved.model.clone(), relation: resolved.relation.clone(), reason: reason.to_string() }
    }

    async fn load_all(&self, resolved: &Resolved<'_>, force: bool) -> Result<(), SyncError> {
        let first = self.fetch(resolved, None, force).await?;
        if let Some(error) = first.kept_error {
            return Err(error);
        }
        match first.status {
            LoadStatus::Discarded(DiscardReason::StaleGeneration) => return Err(Self::incomplete(resolved, "the database was reset")),
            LoadStatus::Discarded(_) => {
                self.flights_settled(resolved).await;
                if self.db.shared.store.window(&resolved.window_key)?.is_none() {
                    return Err(Self::incomplete(resolved, "the first page was discarded and no window is stored"));
                }
            }
            _ => {}
        }
        let RemoteShape::Connection { max_pages, .. } = resolved.remote.shape else { return Ok(()) };
        let mut cursors = HashSet::new();
        loop {
            self.flights_settled(resolved).await;
            let Some(window) = self.db.shared.store.window(&resolved.window_key)? else { return Err(Self::incomplete(resolved, "the window is gone")) };
            let chain = PageChain { cursor: window.cursor.clone(), has_next: window.has_next, loaded_pages: usize::try_from(window.pages).map_err(|_| SyncError::Store("invalid stored window page count".into()))?, is_loading: self.is_loading(resolved) };
            match LoadMoreVerdict::of(&chain, max_pages.map(|pages| pages as usize)) {
                LoadMoreVerdict::AlreadyLoading => continue,
                LoadMoreVerdict::NoCursor if window.has_next => return Err(Self::incomplete(resolved, "the server reports a next page without a cursor")),
                LoadMoreVerdict::NoCursor => return Ok(()),
                LoadMoreVerdict::MaxPagesReached => return Err(Self::incomplete(resolved, "max_pages is reached and more pages remain")),
                LoadMoreVerdict::FetchNextPage => {}
            }
            if !cursors.insert(window.cursor.clone()) {
                return Err(Self::incomplete(resolved, "the cursor did not advance"));
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
        let _app = self.app_load();
        let resolved = self.inner.resolve(crate::lifecycle::Origin::Explicit, model, relation, params)?;
        if resolved.media.is_some() { return self.inner.load_members(&resolved, options.force).await; }
        self.inner.load_all(&resolved, options.force).await
    }
}
