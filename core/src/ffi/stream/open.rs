use std::sync::{Arc, Mutex, OnceLock};

use serde_json::{Map, Value};

use crate::db::{Feed, ObserverHandle};
use crate::identity::RowId;
use crate::query::{Query, Where};
use crate::sync::{LoadOptions, ModelEvents, RefreshError, Sync};
use crate::transport::CancelToken;
use super::super::database::executor;
use super::super::related::RelatedProjection;
use super::super::{Database, DbError, WireBlobSnapshot, WireModelEvent};
use super::{Frame, Stream, StreamRequest, StreamValue};

/// What keeps a stream's reads alive. Dropping it stops them.
#[derive(Default)]
pub(in crate::ffi) struct Handles {
    observer: Option<ObserverHandle>,
    watch: Option<crate::sync::Watch>,
    tasks: Vec<tokio::task::AbortHandle>,
    fetch: Option<(Sync, Arc<Mutex<Option<CancelToken>>>)>,
    events: Option<Arc<OnceLock<Arc<ModelEvents>>>>,
    loading: bool,
}

#[cfg(test)]
impl Handles {
    pub(super) fn watches(&self) -> bool { self.watch.is_some() }

    pub(super) fn loads(&self) -> bool { self.loading }
}

impl Drop for Handles {
    fn drop(&mut self) {
        for task in self.tasks.drain(..) { crate::db::lock::effect(|| task.abort()); }
        if let Some((sync, token)) = self.fetch.take() {
            let token = crate::db::locked(&token).take();
            if let Some(token) = token { sync.cancel_entity_refresh(&token); }
        }
        if let Some(events) = self.events.take().and_then(|cell| cell.get().cloned()) { events.close(); }
        drop((self.observer.take(), self.watch.take()));
    }
}

fn follow_failures(watch: Option<&mut crate::sync::Watch>, stream: Arc<Stream>) -> Option<tokio::task::AbortHandle> {
    Some(follow(watch?.failures()?, stream))
}

pub(in crate::ffi) fn follow(failures: crate::sync::Failures, stream: Arc<Stream>) -> tokio::task::AbortHandle {
    let crate::sync::Failures { current, recheck, mut receiver, sources } = failures;
    let post = |stream: &Stream, failure: crate::transport::SubscriptionFailure| stream.subscription_failed(Some(DbError::from(crate::sync::SyncError::Transport(failure.error))));
    let mut failed = current.is_some();
    if let Some(failure) = current { post(&stream, failure); }
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(None) => { failed = false; stream.subscription_failed(None); }
                Ok(Some((source, failure))) if sources.contains(&source) && !failed => { post(&stream, failure); failed = true; }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => match recheck() {
                    Some(failure) if !failed => { post(&stream, failure); failed = true; }
                    None if failed => { failed = false; stream.subscription_failed(None); }
                    _ => {}
                },
                _ => {}
            }
        }
    }).abort_handle()
}

impl Database {
    pub(super) fn open_stream(&self, stream: &Arc<Stream>, request: &StreamRequest) -> Result<Handles, DbError> {
        match request {
            StreamRequest::Query { query_json } => self.connect_json(query_json, false, None, stream),
            StreamRequest::Count { query_json } => self.connect_json(query_json, true, None, stream),
            StreamRequest::Related { query_json, name } => self.connect_json(query_json, false, Some(name.clone()), stream),
            StreamRequest::Entity { model, id, fetch } => self.connect_entity(stream, model, id, *fetch),
            StreamRequest::Events { model } => self.connect_events(stream, model),
            StreamRequest::Blob { id } => self.connect_blob(stream, id),
        }
    }

    fn connect_entity(&self, stream: &Arc<Stream>, model: &str, id: &str, fetch: crate::sync::EntityFetch) -> Result<Handles, DbError> {
        let row = RowId::new(id).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
        let mut handles = self.connect(Query::new(model).filtered(Where::equal("id", serde_json::json!(row))), None, false, None, LoadOptions::default(), stream)?;
        if fetch == crate::sync::EntityFetch::None { return Ok(handles); }
        let sync = self.sync().map_err(|_| DbError::Invalid { detail: "entity fetch needs a network session".into() })?.clone();
        stream.fetches_entity();
        stream.expect_load();
        let token: Arc<Mutex<Option<CancelToken>>> = Arc::default();
        let start = {
            let (sync, stream, token, model, id) = (sync.clone(), Arc::downgrade(stream), token.clone(), model.to_string(), id.to_string());
            move || -> Result<(), DbError> {
                let waiting = stream.clone();
                let sink: crate::sync::RefreshSink = Arc::new(move |_, error| {
                    if let Some(stream) = waiting.upgrade() {
                        stream.load_finished(error.map(|error| match error {
                            RefreshError::Offline => DbError::Network { detail: "offline".into() },
                            RefreshError::Failed(message) => DbError::Sync { detail: message },
                        }));
                    }
                });
                let _entered = executor()?.enter();
                let cancel = sync.refresh_entities(&model, std::slice::from_ref(&id), fetch, sink)?;
                let replaced = crate::db::locked(&token).replace(cancel);
                if let Some(old) = replaced { sync.cancel_entity_refresh(&old); }
                Ok(())
            }
        };
        start()?;
        let again: Arc<dyn Fn() + Send + std::marker::Sync> = Arc::new(move || { let _ = start(); });
        stream.on_cleared(again);
        handles.fetch = Some((sync, token));
        Ok(handles)
    }

    /// Runs `reload` once each time the network returns after it went away. A stream that opened while the network was away loads again through the failed-load retry of the sync, not through this task.
    fn reload_on_reconnect(&self, reload: Arc<dyn Fn() + Send + std::marker::Sync>) -> Result<tokio::task::AbortHandle, DbError> {
        let connectivity = self.db.connectivity();
        let mut skip = !connectivity.is_online();
        let mut returns = connectivity.returns();
        let task = executor()?.spawn(async move {
            while returns.changed().await.is_ok() {
                if std::mem::take(&mut skip) { continue; }
                reload();
            }
        });
        Ok(task.abort_handle())
    }

    fn connect_events(&self, stream: &Arc<Stream>, model: &str) -> Result<Handles, DbError> {
        let sync = self.sync()?.clone();
        let wire = self.wire_of(model)?;
        let cell: Arc<OnceLock<Arc<ModelEvents>>> = Arc::default();
        let (opened, weak, model) = (cell.clone(), Arc::downgrade(stream), model.to_string());
        let task = executor()?.spawn(async move {
            let events = match sync.open_model_events(&model).await {
                Ok(events) => Arc::new(events),
                Err(error) => {
                    if let Some(stream) = weak.upgrade() { stream.invalid(DbError::from(error)); }
                    return;
                }
            };
            let _ = opened.set(events.clone());
            if let Some(stream) = weak.upgrade() { stream.ready(); }
            while let Some(item) = events.next().await {
                let Some(stream) = weak.upgrade() else { return };
                stream.push(match item {
                    crate::sync::ModelEventItem::Missed(count) => StreamValue::Missed { count },
                    crate::sync::ModelEventItem::Event(event) => StreamValue::Event { event: WireModelEvent { area: event.area, kind: event.kind, id: event.id, before: event.before.as_ref().map(|row| wire.row(row)), after: event.after.as_ref().map(|row| wire.row(row)) } },
                });
            }
            if let Some(stream) = weak.upgrade() { stream.ended_outside(); }
        });
        let mut handles = Handles::default();
        handles.tasks.push(task.abort_handle());
        handles.events = Some(cell);
        Ok(handles)
    }

    fn connect_blob(&self, stream: &Arc<Stream>, id: &str) -> Result<Handles, DbError> {
        let sync = self.sync()?.clone();
        let mut changes = self.db.outbox_changes();
        let (weak, id) = (Arc::downgrade(stream), id.to_string());
        let task = executor()?.spawn(async move {
            let mut last: Option<WireBlobSnapshot> = None;
            loop {
                changes.borrow_and_update();
                let Some(stream) = weak.upgrade() else { return };
                match sync.blob_snapshot(&id) {
                    Ok(blob) => {
                        let next = WireBlobSnapshot::from(blob);
                        if last.as_ref() != Some(&next) {
                            last = Some(next.clone());
                            stream.push(StreamValue::Status { blob: next });
                        }
                    }
                    Err(error) => { stream.invalid(DbError::from(error)); return; }
                }
                drop(stream);
                if changes.changed().await.is_err() { return; }
            }
        });
        let mut handles = Handles::default();
        handles.tasks.push(task.abort_handle());
        Ok(handles)
    }

    fn connect_json(&self, json: &str, count: bool, related: Option<String>, stream: &Arc<Stream>) -> Result<Handles, DbError> {
        let (query, remote) = self.parsed_query(json)?;
        let description: Value = serde_json::from_str(json)?;
        let force = description.get("refreshOnConnect").map(|value| serde_json::from_value(value.clone())).transpose()?.unwrap_or(false);
        self.connect(query, remote, count, related, LoadOptions { force }, stream)
    }

    fn connect(&self, mut query: Query, remote: Option<(String, Map<String, Value>)>, count: bool, related: Option<String>, options: LoadOptions, stream: &Arc<Stream>) -> Result<Handles, DbError> {
        let _entered = executor()?.enter();
        let model = query.model.to_string();
        let projection = related.map(|name| -> Result<_, DbError> {
            let association = self.db.spec(&model)?.associations.iter().find(|association| association.name == name).ok_or_else(|| DbError::Invalid { detail: format!("{model} has no association {name}") })?;
            let (target, kind) = (association.target.to_string(), association.kind);
            query.include = vec![name.clone()];
            stream.show_targets_only();
            Ok((Mutex::new(RelatedProjection::new(name, kind, self.db.shared.counters.clone())), self.wire(&target)?.clone()))
        }).transpose()?;
        let mut tracked = None;
        let mut watch = match remote {
            Some((relation, params)) => {
                let sync = self.sync()?.clone();
                let watch = sync.watch(&model, &relation, &params)?;
                if self.db.spec(&model)?.relations.iter().any(|declared| declared.name == relation && declared.remote.is_some()) {
                    stream.expect_load();
                    tracked = Some((sync, relation, params));
                }
                Some(watch)
            }
            None => None,
        };
        let wire = self.wire(&model)?.clone();
        let sink = Arc::downgrade(stream);
        let observer = self.db.observe_query_feed(query, count, tracked.is_some(), move |item| {
            let Some(stream) = sink.upgrade() else { return };
            match (&projection, item) {
                (Some((projection, target)), Feed::Rows(change)) => {
                    let shown = projection.lock().unwrap_or_else(std::sync::PoisonError::into_inner).apply(&change, target);
                    match shown {
                        Some(targets) => stream.rows(targets),
                        None => stream.reached(change.revision),
                    }
                }
                (Some(_), Feed::Count(_)) => {}
                (_, Feed::Rows(change)) => stream.rows(Frame::rows(&change, &wire)),
                (_, Feed::Count(change)) => stream.count(Frame::counted(change.revision, change.count)),
                (_, Feed::Reached(revision)) => stream.reached(revision),
                (_, Feed::Failed(detail)) => stream.read_failed(detail),
            }
        })?;
        stream.on_rejoin(Arc::new(observer.rejoiner()));
        let mut handles = Handles::default();
        if let (true, Some((sync, relation, params))) = (options.force, tracked.clone()) {
            let model = model.clone();
            handles.tasks.push(self.reload_on_reconnect(Arc::new(move || {
                let (sync, model, relation, params) = (sync.clone(), model.clone(), relation.clone(), params.clone());
                drop(tokio::spawn(async move { let _ = sync.refresh(&model, &relation, &params).await; }));
            }))?);
        }
        let (load, state) = self.follow_load(tracked, model, options, stream, observer.control())?;
        handles.loading = load.is_some();
        handles.tasks.extend(load);
        handles.tasks.extend(state);
        handles.tasks.extend(follow_failures(watch.as_mut(), stream.clone()));
        handles.observer = Some(observer);
        handles.watch = watch;
        Ok(handles)
    }

    fn follow_load(&self, tracked: Option<(Sync, String, Map<String, Value>)>, model: String, options: LoadOptions, stream: &Arc<Stream>, control: crate::db::ObserverControl) -> Result<(Option<tokio::task::AbortHandle>, Option<tokio::task::AbortHandle>), DbError> {
        let Some((sync, relation, params)) = tracked else { return Ok((None, None)) };
        let mut transitions = self.streams.watch();
        stream.load_of(model.clone());
        let ask = control.clone();
        stream.on_ahead(Arc::new(move |revision| ask.want_reach(revision)));
        let (follower, loader) = (stream.clone(), stream.clone());
        let (ready, seeded) = tokio::sync::oneshot::channel();
        let state = executor()?.spawn(async move {
            let mut changes = match sync.relation_state(&model, &relation, &params).await {
                Ok(changes) => changes,
                Err(error) => {
                    follower.load_finished(Some(DbError::from(error)));
                    return control.resume();
                }
            };
            let seed = changes.borrow_and_update().clone();
            follower.seed(seed);
            control.resume();
            crate::db::lock::effect(|| { let _ = ready.send((sync, model, relation, params, changes.clone())); });
            while changes.changed().await.is_ok() {
                let current = changes.borrow_and_update().clone();
                follower.relation_changed(current);
            }
        }).abort_handle();
        let load = executor()?.spawn(async move {
            let Ok((sync, model, relation, params, receiver)) = seeded.await else { return };
            let mut force = options.force;
            loop {
                let snapshot = transitions.current();
                if snapshot.closed { return; }
                if !snapshot.admits(crate::lifecycle::Origin::Session) {
                    loader.load_finished(None);
                    if transitions.changed().await.is_none() { return; }
                    continue;
                }
                let resolved = match sync.inner.resolve(crate::lifecycle::Origin::Session, &model, &relation, &params) {
                    Ok(resolved) => resolved,
                    Err(error) => { loader.load_finished(Some(DbError::from(error))); return; }
                };
                let app_load = sync.app_load();
                loader.reset_load();
                let concerning = async {
                    loop {
                        match transitions.changed().await {
                            None => break None,
                            Some(next) if next.concerns(&model) => break Some(next),
                            Some(_) => {}
                        }
                    }
                };
                let result = tokio::select! {
                    biased;
                    changed = concerning => {
                        if changed.is_none() { return; }
                        continue;
                    }
                    result = sync.inner.load_scope(&resolved, force) => result,
                };
                force = false;
                let settled = receiver.borrow().clone();
                loader.relation_changed(settled);
                loader.load_finished(result.err().map(DbError::from));
                drop(app_load);
                loop {
                    let Some(next) = transitions.changed().await else { return };
                    if next.closed { return; }
                    if next.concerns(&model) { break; }
                }
            }
        }).abort_handle();
        Ok((Some(load), Some(state)))
    }
}
