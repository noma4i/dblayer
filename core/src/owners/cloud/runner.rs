use std::sync::{Arc, Weak};
use crate::ffi::{CloudConfig, CloudIssueCause, CloudOutbound, CloudSendStatus, CloudSignals, CloudWireScope, DbError};
use crate::owners::contract::Drained;
use crate::owners::life::Wiring;
use crate::owners::lifecycle::LifecycleInput;
use crate::runtime::{Back, EffectRunner, EpochStamp};
use super::{Cloud, CloudEffect, CloudInput, CloudScope};

pub(crate) struct Active {
    scope: CloudScope,
    prepare: tokio::task::JoinHandle<()>,
    start: tokio::task::JoinHandle<()>,
    send: Option<tokio::task::JoinHandle<()>>,
    bootstrap: Option<tokio::task::JoinHandle<()>>,
    cancel: crate::transport::CancelToken,
}

pub(crate) struct CloudRunner {
    pub port: Arc<dyn crate::ffi::CloudPort>,
    pub signals: Arc<CloudSignals>,
    pub executor: tokio::runtime::Handle,
    pub wiring: Weak<Wiring>,
    pub active: Option<Active>,
    pub db: Arc<crate::store::Store>,
    pub standalone: bool,
    pub status: tokio::sync::watch::Sender<crate::ffi::CloudStatus>,
}

pub(crate) fn signals(_config: &CloudConfig, wiring: Weak<Wiring>, db: Arc<crate::store::Store>, executor: tokio::runtime::Handle) -> Arc<CloudSignals> {
    let fetch_wiring = wiring.clone();
    let relay_wiring = wiring.clone();
    Arc::new(CloudSignals::new(move |account| {
        let Some(wiring) = wiring.upgrade() else { return; };
        wiring.to_cloud(CloudInput::Provider(account));
    }, move |fetched| {
        let (weak, db, executor) = (fetch_wiring.clone(), db.clone(), executor.clone());
        Box::pin(async move {
            let wiring = weak.upgrade().ok_or_else(|| DbError::Closed { detail: "database closed".into() })?;
            let launch = wiring.handle.launch();
            let scope = CloudScope { epoch: fetched.scope.epoch, generation: fetched.scope.generation, domain: fetched.scope.domain, provider_fingerprint: fetched.scope.provider_fingerprint };
            let generation = scope.generation;
            let initial_complete = fetched.initial_complete;
            let result = crate::task::spawn_blocking_on(&executor, Some(launch), move || {
                let records = fetched.records.into_iter().map(|record| {
                    crate::cloud::decode(&record.key, &record.payload).map(|body| (body, record.system_fields)).map_err(|_| DbError::Invalid { detail: "invalid cloud record".into() })
                }).collect::<Result<Vec<_>, _>>()?;
                db.land_local_cloud(&scope, records, fetched.progress.as_deref()).map_err(DbError::from)
            }).await.map_err(|error| DbError::Runtime { detail: error.to_string() })?;
            result?;
            if initial_complete { wiring.to_cloud(CloudInput::InitialRead { generation }); }
            Ok(())
        })
    }, move |scope, event| {
        let Some(wiring) = relay_wiring.upgrade() else { return; };
        let scope = CloudScope { epoch: scope.epoch, generation: scope.generation, domain: scope.domain, provider_fingerprint: scope.provider_fingerprint };
        wiring.to_cloud(CloudInput::Relay { scope, event });
    }))
}

impl EffectRunner<Cloud> for CloudRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: CloudEffect, back: &Back<Cloud>) {
        match effect {
            CloudEffect::Start(scope) => {
                let (port, signals, back, weak) = (self.port.clone(), self.signals.clone(), back.clone(), self.wiring.clone());
                let Some(wiring) = self.wiring.upgrade() else { return; };
                let db = self.db.clone();
                let prepare_scope = scope.clone();
                let (tx, rx) = tokio::sync::oneshot::channel();
                let prepare = crate::task::spawn_blocking_on(&self.executor, Some(wiring.handle.launch()), move || {
                    let result = db.activate_cloud_scope(prepare_scope.clone()).and_then(|()| db.cloud_resume(&prepare_scope));
                    let _ = crate::lock::effect(|| tx.send(result));
                });
                let launch = wiring.handle.launch();
                let generation = scope.generation;
                let wire = CloudWireScope { epoch: scope.epoch, generation, domain: scope.domain.clone(), provider_fingerprint: scope.provider_fingerprint.clone() };
                let gate = wiring.gate.clone();
                let epoch = (!self.standalone).then_some(scope.epoch);
                let start = crate::task::spawn_on(&self.executor, crate::runtime::counted(Some(launch), async move {
                    let result: Result<(), String> = match rx.await {
                        Ok(Ok(progress)) => crate::runtime::send_in_epoch(&gate, epoch, || async move {
                            crate::lock::effect(|| port.start(wire, progress, signals)).await.map_err(|error| crate::transport::TransportError::Internal(error.to_string()))
                        }).await.map_err(|error| error.to_string()),
                        Ok(Err(error)) => Err(error.to_string()),
                        Err(error) => Err(error.to_string()),
                    };
                    if let Err(error) = result {
                        if let Some(wiring) = weak.upgrade() { wiring.fault(format!("cloud start: {error}")); }
                        back.reply(EpochStamp::Control, CloudInput::StartFailed { generation });
                    }
                }));
                self.active = Some(Active { scope, prepare, start, send: None, bootstrap: None, cancel: crate::transport::CancelToken::new() });
            }
            CloudEffect::Bootstrap(scope) => {
                let Some(wiring) = self.wiring.upgrade() else { return; };
                let Some(active) = self.active.as_mut().filter(|active| active.scope == scope) else { return; };
                let (db, back, executor, counted, weak) = (self.db.clone(), back.clone(), self.executor.clone(), wiring.handle.clone(), self.wiring.clone());
                let cancel = active.cancel.clone();
                let launch = counted.launch();
                active.bootstrap = Some(crate::task::spawn_on(&self.executor, crate::runtime::counted(Some(launch), async move {
                    let generation = scope.generation;
                    loop {
                        let (db, scope) = (db.clone(), scope.clone());
                        let result = crate::task::spawn_blocking_on(&executor, Some(counted.launch()), move || db.bootstrap_cloud_chunk(&scope)).await;
                        if cancel.is_cancelled() { break; }
                        match result {
                            Ok(Ok(false)) => continue,
                            Ok(Ok(true)) => { back.reply(EpochStamp::Control, CloudInput::BootstrapDone { generation }); break; }
                            error => {
                                if let Some(wiring) = weak.upgrade() { wiring.fault(format!("cloud bootstrap: {error:?}")); }
                                back.reply(EpochStamp::Control, CloudInput::StartFailed { generation });
                                break;
                            }
                        }
                    }
                })));
            }
            CloudEffect::Send(scope) => {
                let Some(wiring) = self.wiring.upgrade() else { return; };
                let Some(active) = self.active.as_mut().filter(|active| active.scope == scope) else { return; };
                let (db, port, back, executor) = (self.db.clone(), self.port.clone(), back.clone(), self.executor.clone());
                let (gate, counted, weak) = (wiring.gate.clone(), wiring.handle.clone(), self.wiring.clone());
                let epoch = (!self.standalone).then_some(scope.epoch);
                let launch = counted.launch();
                active.send = Some(crate::task::spawn_on(&self.executor, crate::runtime::counted(Some(launch), async move {
                    let generation = scope.generation;
                    let result = send_pass(db, port, scope, gate, epoch, executor, counted, back.clone()).await;
                    let (more, retry) = match result {
                        Ok(result) => result,
                        Err(error) => {
                            if let Some(wiring) = weak.upgrade() { wiring.fault(format!("cloud send: {error}")); }
                            (false, true)
                        }
                    };
                    back.reply(EpochStamp::Control, CloudInput::SendDone { generation, more, retry });
                })));
            }
            CloudEffect::Stop { generation } => {
                let (port, back) = (self.port.clone(), back.clone());
                let Some(wiring) = self.wiring.upgrade() else { return; };
                let launch = wiring.handle.launch();
                let active = self.active.take().filter(|active| active.scope.generation == generation);
                if let Some(active) = &active { active.cancel.cancel(); }
                if let Some(active) = &active { crate::lock::effect(|| active.start.abort()); }
                if let Some(send) = active.as_ref().and_then(|active| active.send.as_ref()) { crate::lock::effect(|| send.abort()); }
                let db = self.db.clone();
                drop(crate::task::spawn_on(&self.executor, crate::runtime::counted(Some(launch), async move {
                    if let Some(active) = active {
                        db.retire_cloud_scope(&active.scope);
                        let _ = active.start.await;
                        if let Some(send) = active.send { let _ = send.await; }
                        if let Some(bootstrap) = active.bootstrap { let _ = bootstrap.await; }
                        let _ = active.prepare.await;
                        db.retire_cloud_scope(&active.scope);
                    }
                    port.stop(generation).await;
                    back.reply(EpochStamp::Control, CloudInput::Stopped { generation });
                })));
            }
            CloudEffect::Drained => {
                let Some(wiring) = self.wiring.upgrade() else { return; };
                let port = self.port.clone();
                let launch = wiring.handle.launch();
                drop(crate::task::spawn_on(&self.executor, crate::runtime::counted(Some(launch), async move {
                    crate::lock::effect(|| port.unobserve()).await;
                    wiring.to_lifecycle(LifecycleInput::Drained(Drained { owner: "cloud" }));
                })));
            }
            CloudEffect::Erase { domain, token } => {
                let Some(wiring) = self.wiring.upgrade() else { return; };
                let (db, back, weak) = (self.db.clone(), back.clone(), self.wiring.clone());
                drop(crate::task::spawn_blocking_on(&self.executor, Some(wiring.handle.launch()), move || {
                    let input = match db.erase_cloud_domain(&domain) {
                        Ok(()) => CloudInput::Erased { token },
                        Err(error) => {
                            if let Some(wiring) = weak.upgrade() { wiring.fault(format!("cloud erase: {error}")); }
                            CloudInput::EraseFailed { token }
                        }
                    };
                    back.reply(EpochStamp::Control, input);
                }));
            }
            CloudEffect::TokenExhausted => {
                if let Some(wiring) = self.wiring.upgrade() { wiring.fault("cloud generation exhausted".into()); }
            }
            CloudEffect::Ready(_) => {}
            CloudEffect::Publish(status) => { crate::lock::effect(|| self.status.send_replace(status)); }
        }
    }
}

async fn send_pass(
    db: Arc<crate::store::Store>, port: Arc<dyn crate::ffi::CloudPort>, scope: CloudScope,
    gate: Arc<crate::runtime::EpochGate>, epoch: Option<u64>, executor: tokio::runtime::Handle, counted: crate::runtime::Handle, back: Back<Cloud>,
) -> Result<(bool, bool), DbError> {
    let generation = scope.generation;
    let read_db = db.clone();
    let read_scope = scope.clone();
    let (valid, outgoing, full, blocked, issues) = crate::task::spawn_blocking_on(&executor, Some(counted.launch()), move || -> Result<_, DbError> {
        let records = read_db.pending_cloud_records(&read_scope, 16)?;
        let full = records.len() == 16;
        let mut outgoing = Vec::new();
        let mut valid = Vec::new();
        let mut blocked = false;
        for record in records {
            match &record.payload {
                Ok(payload) => outgoing.push(CloudOutbound { key: record.key.clone(), version: record.version, payload: payload.clone(), system_fields: record.system_fields.clone() }),
                Err(crate::cloud::RecordError::TooLarge) => {
                    read_db.block_cloud_send(&read_scope, &record, CloudIssueCause::TooLarge)?;
                    blocked = true;
                    continue;
                }
                Err(crate::cloud::RecordError::Invalid) => return Err(DbError::Invalid { detail: "invalid cloud materialization".into() }),
            }
            valid.push(record);
        }
        let issues = read_db.cloud_issues(&read_scope)?;
        Ok((valid, outgoing, full, blocked, issues))
    }).await.map_err(|error| DbError::Runtime { detail: error.to_string() })??;
    back.reply(EpochStamp::Control, CloudInput::Issues { generation: scope.generation, issues });
    if outgoing.is_empty() { return Ok((full && blocked, false)); }
    let wire = CloudWireScope { epoch: scope.epoch, generation: scope.generation, domain: scope.domain.clone(), provider_fingerprint: scope.provider_fingerprint.clone() };
    let results = crate::runtime::send_in_epoch(&gate, epoch, || async move {
        crate::lock::effect(|| port.send(wire, outgoing)).await.map_err(|error| crate::transport::TransportError::Internal(error.to_string()))
    }).await.map_err(DbError::from)?;
    crate::task::spawn_blocking_on(&executor, Some(counted.launch()), move || -> Result<_, DbError> {
      let mut progress = blocked;
      let mut retry = false;
      for record in valid {
        let mut matching = results.iter().filter(|result| result.key == record.key && result.version == record.version);
        let Some(result) = matching.next() else { retry = true; continue; };
        if matching.next().is_some() { retry = true; continue; }
        match &result.status {
            CloudSendStatus::Saved { system_fields } => { db.confirm_cloud_send(&scope, &record, system_fields.as_deref())?; progress = true; }
            CloudSendStatus::Failed { .. } => {
                db.block_cloud_send(&scope, &record, CloudIssueCause::Rejected)?;
                progress = true;
            }
            CloudSendStatus::Retryable { .. } => retry = true,
            CloudSendStatus::Conflict { record: incoming } => {
                if incoming.key != record.key { retry = true; continue; }
                let body = crate::cloud::decode(&incoming.key, &incoming.payload).map_err(|_| DbError::Invalid { detail: "invalid cloud record".into() })?;
                db.land_local_cloud(&scope, vec![(body, incoming.system_fields.clone())], None)?;
                retry = true;
            }
        }
      }
      let issues = db.cloud_issues(&scope)?;
      Ok((full && progress, retry, issues))
    }).await.map_err(|error| DbError::Runtime { detail: error.to_string() })?.map(|(more, retry, issues)| {
        back.reply(EpochStamp::Control, CloudInput::Issues { generation, issues });
        (more, retry)
    })
}
