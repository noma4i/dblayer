//! Executor of the effects of `Transfers`: the copy of the source in chunks of 1 MiB outside the writer,
//! the steps of the file (PUT, multipart, complete, abort) with `NetworkPurpose::Transfer`, the answers of calls, the view of `uploads[action]`, the platform job.
//! The result of an effect is an input to `Transfers` through `Back` and nothing else. Store batches, inputs to Outbox and the retry timer
//! are `Effect::Send` and `Effect::Schedule` of the owner.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use super::failure;
use super::transfer::{file_id, prepared_file_id, staged_file_id, AbortFault, CachedBlob, StatusSink, Source, Steps, TransferAnswer, TransferEffect, TransferId, TransferInput, Transfers};
use super::wire::{Flights, Running, Wire};
use crate::store::files::{CopiedSource, FileDirectories};
use crate::owners::contract::{Drained, ReplyId};
use crate::runtime::{Back, EffectRunner, Epoch, EpochStamp};
use crate::transport::{CancelToken, GraphQLRequest, NetworkPurpose, PutRequest, PutReply, Response, RetryClass, TransportError};

/// The requests of one step. Every request goes out with `NetworkPurpose::Transfer` and stops with the step.
pub struct Link {
    cancel: CancelToken,
    gate: Arc<crate::runtime::EpochGate>,
    epoch: Option<Epoch>,
    minted: Option<Arc<dyn Fn(String) + Send + Sync>>,
    /// The cycle and the part of the step the link serves: with the key and the step they give the request id.
    identity: (u64, u64, u32),
    /// The answer of the saved pair of a restored step: the first request under its id gets it without the network.
    seeded: Option<(u64, Result<Response, TransportError>)>,
    seed_used: std::sync::atomic::AtomicBool,
    /// Where the bytes of a PUT in flight go: the owner of the transfer.
    progress: Option<Arc<dyn Fn(u64) + Send + Sync>>,
}

impl Drop for Link {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("Link"); }
}

impl Link {
    #[cfg(test)]
    pub(super) fn over(gate: Arc<crate::runtime::EpochGate>, epoch: Epoch) -> Self { Self { cancel: CancelToken::new(), gate, epoch: Some(epoch), minted: None, identity: (0, 1, 0), seeded: None, seed_used: Default::default(), progress: None } }

    /// `step` names the request: its id is the function of the step, so a repeat of the step carries it again.
    pub async fn mutate(&self, mut request: GraphQLRequest, step: &crate::transport::RequestOwner) -> Result<Response, TransportError> {
        request.purpose = NetworkPurpose::Transfer;
        let id = match step {
            crate::transport::RequestOwner::Orphan { upload_id } => super::wire::abort_request_id(upload_id),
            crate::transport::RequestOwner::Transfer { step, .. } => super::wire::step_request_id(self.identity.0, self.identity.1, *step, self.identity.2),
            _ => crate::owners::contract::next_request_id(),
        };
        if let Some((seeded_id, response)) = self.seeded.as_ref().filter(|(seeded_id, _)| *seeded_id == id) {
            if !self.seed_used.swap(true, std::sync::atomic::Ordering::SeqCst) {
                let _ = seeded_id;
                return response.clone();
            }
        }
        let request = request.by(crate::transport::WireIds::new(crate::transport::Author::Transfer, id, crate::transport::WireIds::next_attempt()));
        let mut response = self.gate.mutate(self.epoch, request).await?;
        if response.kind.is_none() {
            response.kind = Some(response.answer_kind().map_err(|error| TransportError::Decode(error.to_string()))?);
        }
        Ok(response)
    }

    pub fn minted(&self, upload_id: String) {
        if let Some(report) = &self.minted {
            report(upload_id);
        }
    }

    /// Reports to the owner once per hundredth of the range: the owner gets at most 100 inputs per PUT.
    fn hundredths(report: Arc<dyn Fn(u64) + Send + Sync>, length: u64) -> crate::transport::ByteSink {
        let (step, last) = (length / 100, std::sync::atomic::AtomicU64::new(0));
        Arc::new(move |sent| {
            if sent >= last.load(std::sync::atomic::Ordering::Relaxed) + step.max(1) || sent >= length {
                last.store(sent, std::sync::atomic::Ordering::Relaxed);
                report(sent);
            }
        })
    }

    pub async fn put(&self, mut request: PutRequest) -> Result<PutReply, TransportError> {
        request.cancel = self.cancel.clone();
        if let Some(report) = self.progress.clone() { request.sent = Self::hundredths(report, request.file.length); }
        self.gate.put(self.epoch, request).await
    }
}

/// What a step brings back: `upload_id` of a multipart upload the step created.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StepDone {
    pub upload_id: Option<String>,
    pub etag: Option<String>,
}

/// Encoding of the requests of a step and decoding of the answers. It is stateless: everything it needs comes by value from the effect.
#[async_trait]
pub(crate) trait StepCodec: Send + Sync + 'static {
    /// Step `part` (`None` = a single PUT). The first step of a multipart upload creates it and returns `upload_id`.
    async fn step(&self, link: &Link, operation: u64, part: Option<u32>, upload_id: Option<&str>, source: &Source, steps: &Steps) -> Result<StepDone, TransportError>;
    /// The answer of `complete` carries `signed_id` of the file on the server.
    async fn complete(&self, link: &Link, key: &str, upload_id: Option<&str>, parts: &[(u32, Option<String>)]) -> Result<Option<String>, TransportError>;
    async fn discard_upload(&self, link: &Link, upload_id: Option<&str>) -> Result<(), TransportError>;
}

/// What Life needs to install Transfer and Rounds: the codec of the steps, the platform job of the round.
pub(crate) struct TransferPort {
    pub codec: Arc<dyn StepCodec>,
    pub jobs: Arc<dyn super::rounds_executor::RoundJobs>,
    /// `FileSlots`: transfers in Uploading and Completing at the same time.
    pub slots: u32,
    pub put_timeout: std::time::Duration,
    /// The root of the abort operation in its answer: only an answer that carries it confirms an abort.
    pub abort_root: Option<String>,
    pub sizing: super::upload::Sizing,
    /// The platform that probes and writes the sources with prepare settings: it is set once, when the platform gives it to `open`.
    pub preparer: Arc<std::sync::OnceLock<Arc<dyn crate::ffi::Preparer>>>,
}

type Call<T> = Pin<Box<dyn Future<Output = Result<T, TransportError>> + Send>>;

struct FileReplies {
    settle: Arc<dyn Fn(ReplyId, TransferAnswer) + Send + Sync>,
    answers: Vec<(ReplyId, TransferAnswer)>,
}

impl Drop for FileReplies {
    fn drop(&mut self) {
        for (reply, answer) in std::mem::take(&mut self.answers) { (self.settle)(reply, answer); }
    }
}

/// What a step asks of the codec.
/// The saved answer of the pair of a restored step as the response its first request gets.
fn seeded_response(seeded: Option<super::transfer_types::SeededAnswer>) -> Option<(u64, Result<Response, TransportError>)> {
    let (id, answer) = seeded?;
    let response = answer.map_err(crate::transport::encrypted::exchange::session_failure).and_then(|(body, kind)| {
        let body = serde_json::from_str(&body).map_err(|error| TransportError::Decode(error.to_string()))?;
        crate::transport::encrypted::response_from_answer_with_kind(crate::transport::Answer { sequence: None, body }, kind)
    });
    Some((id, response))
}

struct StepAsk {
    cycle_id: u64,
    operation: u64,
    part: Option<u32>,
    upload_id: Option<String>,
    source: Source,
    steps: Steps,
    key: String,
    seeded: Option<super::transfer_types::SeededAnswer>,
}

/// What the owner asks of Lifecycle and Network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LifeSignal {
    Started(Epoch),
    SnapshotRequest(Epoch),
    RestoreClassified(Epoch),
    ReleaseStep(u64),
}

pub(crate) struct TransferExecutor {
    wire: Wire,
    codec: Arc<dyn StepCodec>,
    files: Arc<FileDirectories>,
    settle: Arc<dyn Fn(ReplyId, TransferAnswer) + Send + Sync>,
    status: StatusSink,
    drained: Arc<dyn Fn(Drained) + Send + Sync>,
    signal: Arc<dyn Fn(LifeSignal) + Send + Sync>,
    copies: Flights<TransferId>,
    steps: Flights<TransferId>,
    prepares: Flights<TransferId>,
    preparer: Arc<std::sync::OnceLock<Arc<dyn crate::ffi::Preparer>>>,
    /// The cache of blobs: what is known of a server blob for the content of a copy.
    cache: Arc<dyn Fn(&str) -> CachedBlob + Send + Sync>,
    doomed: Vec<String>,
    answers: Vec<(ReplyId, TransferAnswer)>,
}

/// Where the executor reports to.
pub struct TransferSinks {
    pub settle: Arc<dyn Fn(ReplyId, TransferAnswer) + Send + Sync>,
    pub status: StatusSink,
    pub drained: Arc<dyn Fn(Drained) + Send + Sync>,
    pub signal: Arc<dyn Fn(LifeSignal) + Send + Sync>,
}

impl TransferExecutor {
    pub fn new(wire: Wire, codec: Arc<dyn StepCodec>, files: Arc<FileDirectories>, sinks: TransferSinks, cache: Arc<dyn Fn(&str) -> CachedBlob + Send + Sync>) -> Self {
        let TransferSinks { settle, status, drained, signal } = sinks;
        Self { wire, codec, files, settle, status, drained, signal, copies: Flights::default(), steps: Flights::default(), prepares: Flights::default(), preparer: Arc::default(), cache, doomed: Vec::new(), answers: Vec::new() }
    }

    pub fn with_preparer(mut self, preparer: Arc<std::sync::OnceLock<Arc<dyn crate::ffi::Preparer>>>) -> Self {
        self.preparer = preparer;
        self
    }

    fn copy(&self, stamp: EpochStamp, back: &Back<Transfers>, (transfer_id, attempt_id): (TransferId, u64), source: Source, staged: bool) { self.copy_into(stamp, back, (transfer_id, attempt_id), source, (staged, None)); }

    /// The result of the platform is read in chunks into the temporary file of the transfer and goes.
    fn measure(&self, stamp: EpochStamp, back: &Back<Transfers>, (transfer_id, attempt_id): (TransferId, u64)) {
        let prepared = prepared_file_id(transfer_id);
        let path = self.files.path(&prepared).unwrap_or_else(|| self.files.cache_target(&prepared, "prepared"));
        self.copy_into(stamp, back, (transfer_id, attempt_id), Source { path: path.to_string_lossy().into_owned(), parts: 0 }, (true, Some(prepared)));
    }

    fn copy_into(&self, stamp: EpochStamp, back: &Back<Transfers>, (transfer_id, attempt_id): (TransferId, u64), source: Source, (staged, consume): (bool, Option<String>)) {
        let cancelled = Arc::new(AtomicBool::new(false));
        let (files, copies, back, flag, id, cache) = (self.files.clone(), self.copies.clone(), back.clone(), cancelled.clone(), if staged { staged_file_id(transfer_id) } else { file_id(transfer_id) }, self.cache.clone());
        let running = self.wire.tasks.start_blocking(Box::new(move || {
            let result = files.copy_source(&id, std::path::Path::new(&source.path));
            if let Some(consumed) = &consume { let _ = files.discard(std::slice::from_ref(consumed)); }
            let input = if flag.load(Ordering::SeqCst) {
                let _ = files.discard(&[id]);
                None
            } else {
                Some(match result {
                    Ok(CopiedSource::Ready { size, sha256, location, data }) => match keep_small_copy(&files, &id, location, &data) {
                        Ok(location) => TransferInput::CopyDone { transfer_id, attempt_id, size, cached: cache(&sha256), sha256, place: location },
                        Err(error) => TransferInput::CopyFailed { transfer_id, attempt_id, error: error.to_string() },
                    },
                    Ok(CopiedSource::Unreadable(error)) => TransferInput::CopyFailed { transfer_id, attempt_id, error },
                    Err(error) => TransferInput::CopyFailed { transfer_id, attempt_id, error: error.to_string() },
                })
            };
            copies.done(&transfer_id);
            if let Some(input) = input {
                back.reply(stamp, input);
            }
        }));
        self.copies.put(transfer_id, Running::new(move || {
            cancelled.store(true, Ordering::SeqCst);
            running.cancel();
        }));
    }

    /// The path of the copy of the library: the platform reads the source from it.
    fn copy_path(&self, transfer_id: TransferId) -> String { self.local(transfer_id, Source { path: String::new(), parts: 0 }).path }

    fn probe(&self, stamp: EpochStamp, back: &Back<Transfers>, (transfer_id, attempt_id): (TransferId, u64), kind: crate::ffi::PrepareKind) {
        let Some(preparer) = self.preparer.get().cloned() else {
            back.reply(stamp, TransferInput::Probed { transfer_id, attempt_id, outcome: crate::ffi::ProbeOutcome::Failed { message: "no preparer".into() } });
            return;
        };
        let (path, prepares, back, cancelled) = (self.copy_path(transfer_id), self.prepares.clone(), back.clone(), Arc::new(AtomicBool::new(false)));
        let flag = cancelled.clone();
        let _ = self.wire.tasks.start(Box::pin(async move {
            let outcome = preparer.probe(path, kind).await;
            prepares.done(&transfer_id);
            if !flag.load(Ordering::SeqCst) { back.reply(stamp, TransferInput::Probed { transfer_id, attempt_id, outcome }); }
        }));
        self.prepares.put(transfer_id, Running::new(move || cancelled.store(true, Ordering::SeqCst)));
    }

    fn prepare(&self, stamp: EpochStamp, back: &Back<Transfers>, (transfer_id, attempt_id): (TransferId, u64), plan: crate::ffi::PreparePlan) {
        let Some(preparer) = self.preparer.get().cloned() else {
            back.reply(stamp, TransferInput::Prepared { transfer_id, attempt_id, outcome: crate::ffi::PrepareOutcome::Failed { message: "no preparer".into() } });
            return;
        };
        let prepared = prepared_file_id(transfer_id);
        let destination = self.files.path(&prepared).unwrap_or_else(|| self.files.cache_target(&prepared, "prepared"));
        if let Some(parent) = destination.parent() { let _ = std::fs::create_dir_all(parent); }
        let job = crate::ffi::PrepareJob { attempt_id, source: self.copy_path(transfer_id), destination: destination.to_string_lossy().into_owned(), plan };
        let progress = { let back = back.clone(); Arc::new(crate::ffi::PrepareProgress::new(move |fraction| back.reply(stamp, TransferInput::PrepareProgress { transfer_id, attempt_id, percent: (fraction.clamp(0.0, 1.0) * 100.0).floor() as u32 }))) };
        let (files, prepares, back, cancelled) = (self.files.clone(), self.prepares.clone(), back.clone(), Arc::new(AtomicBool::new(false)));
        let flag = cancelled.clone();
        let _ = self.wire.tasks.start(Box::pin(async move {
            let _ = files.discard(&[prepared.clone(), staged_file_id(transfer_id)]);
            let outcome = preparer.run(job, progress).await;
            prepares.done(&transfer_id);
            if flag.load(Ordering::SeqCst) { let _ = files.discard(&[prepared]); } else { back.reply(stamp, TransferInput::Prepared { transfer_id, attempt_id, outcome }); }
        }));
        self.prepares.put(transfer_id, Running::new(move || cancelled.store(true, Ordering::SeqCst)));
    }

    fn prepare_cancel(&self, transfer_id: TransferId, attempt_id: u64) {
        self.prepares.cancel(&transfer_id);
        if let Some(preparer) = self.preparer.get() { preparer.cancel(attempt_id); }
    }

    /// The copy of the library when it exists: the original file may be gone after the call of `upload`.
    fn local(&self, transfer_id: TransferId, source: Source) -> Source {
        let id = file_id(transfer_id);
        let copy = self.files.path(&id).filter(|path| path.exists()).or_else(|| Some(self.files.cache_target(&id, "source")).filter(|path| path.exists()));
        match copy {
            Some(path) => Source { path: path.to_string_lossy().into_owned(), ..source },
            None => source,
        }
    }

    fn link(&self, stamp: EpochStamp) -> Option<Link> {
        let epoch = match stamp {
            EpochStamp::Epoch(epoch) => Some(epoch),
            EpochStamp::Control => None,
        };
        if epoch.is_some_and(|epoch| !self.wire.gate.admits(epoch)) { return None; }
        Some(Link { cancel: CancelToken::new(), gate: self.wire.gate.clone(), epoch, minted: None, identity: (0, 1, 0), seeded: None, seed_used: Default::default(), progress: None })
    }

    /// Runs `call` as the task of `transfer_id`: its result becomes `into_input(result)` for the owner, unless the task is cancelled first.
    fn task<T: Send + 'static>(
        &self,
        stamp: EpochStamp,
        back: &Back<Transfers>,
        transfer_id: TransferId,
        no_link: TransferInput,
        call: impl FnOnce(Arc<dyn StepCodec>, Link) -> Call<T> + Send + 'static,
        into_input: impl FnOnce(Result<T, TransportError>) -> TransferInput + Send + 'static,
    ) {
        let Some(link) = self.link(stamp) else {
            back.reply(stamp, no_link);
            return;
        };
        let cancel = link.cancel.clone();
        let (codec, steps, back) = (self.codec.clone(), self.steps.clone(), back.clone());
        let running = self.wire.tasks.start(Box::pin(async move {
            let result = call(codec, link).await;
            steps.done(&transfer_id);
            back.reply(stamp, into_input(result));
        }));
        self.steps.put(transfer_id, Running::new(move || {
            cancel.cancel();
            running.cancel();
        }));
    }

    fn step(&self, stamp: EpochStamp, back: &Back<Transfers>, (transfer_id, attempt_id): (TransferId, u64), ask: StepAsk) {
        let StepAsk { cycle_id, operation, part, upload_id, source, steps, key, seeded } = ask;
        let source = self.local(transfer_id, source);
        let progress = back.clone();
        let report = { let back = back.clone(); move |upload_id| back.reply(stamp, TransferInput::UploadMinted { transfer_id, attempt_id, upload_id, key: key.clone() }) };
        self.task(
            stamp,
            back,
            transfer_id,
            TransferInput::StepError { transfer_id, attempt_id, temporary: true, error: failure::no_link() },
            move |codec, mut link| {
                link.minted = Some(Arc::new(report));
                link.identity = (transfer_id, cycle_id, part.unwrap_or(0));
                link.seeded = seeded_response(seeded);
                link.progress = Some(Arc::new(move |sent| progress.reply(stamp, TransferInput::PutProgress { transfer_id, attempt_id, sent })));
                Box::pin(async move { codec.step(&link, operation, part, upload_id.as_deref(), &source, &steps).await })
            },
            move |result| match result {
                Ok(done) => TransferInput::StepOk { transfer_id, attempt_id, part, upload_id: done.upload_id, etag: done.etag },
                Err(error) => TransferInput::StepError { transfer_id, attempt_id, temporary: temporary(&error), error: failure::of_transport(&error) },
            },
        );
    }

    fn complete(&self, stamp: EpochStamp, back: &Back<Transfers>, (transfer_id, attempt_id, cycle_id): (TransferId, u64, u64), key: String, (upload_id, parts): (Option<String>, Vec<(u32, Option<String>)>), seeded: Option<super::transfer_types::SeededAnswer>) {
        self.task(
            stamp,
            back,
            transfer_id,
            TransferInput::CompleteError { transfer_id, attempt_id, temporary: true, error: failure::no_link() },
            move |codec, mut link| {
                link.seeded = seeded_response(seeded);
                link.identity = (transfer_id, cycle_id, 0);
                Box::pin(async move { codec.complete(&link, &key, upload_id.as_deref(), &parts).await })
            },
            move |result| match result {
                Ok(signed_id) => TransferInput::Completed { transfer_id, attempt_id, signed_id },
                Err(error) => TransferInput::CompleteError { transfer_id, attempt_id, temporary: temporary(&error), error: failure::of_transport(&error) },
            },
        );
    }

    /// The owner has no input for the end of an abort: the result is not reported.
    fn drop_upload(&self, upload_id: String) {
        let Some(link) = self.link(EpochStamp::Control) else { return };
        let codec = self.codec.clone();
        let _ = self.wire.tasks.start(Box::pin(async move {
            let _ = codec.discard_upload(&link, Some(&upload_id)).await;
        }));
    }

    fn abort_orphan(&self, stamp: EpochStamp, back: &Back<Transfers>, upload_id: String) {
        let Some(link) = self.link(stamp) else {
            back.reply(stamp, TransferInput::AbortDone { upload_id, result: Err(AbortFault { temporary: true, status: None }) });
            return;
        };
        let (codec, back) = (self.codec.clone(), back.clone());
        let _ = self.wire.tasks.start(Box::pin(async move {
            let result = abort_result(&codec.discard_upload(&link, Some(&upload_id)).await);
            back.reply(stamp, TransferInput::AbortDone { upload_id, result });
        }));
    }

    fn finish_files(&mut self) {
        let (doomed, answers) = (std::mem::take(&mut self.doomed), std::mem::take(&mut self.answers));
        if doomed.is_empty() {
            for (reply, answer) in answers {
                (self.settle)(reply, answer);
            }
            return;
        }
        let files = self.files.clone();
        let replies = FileReplies { settle: self.settle.clone(), answers };
        let _ = self.wire.tasks.start_blocking(Box::new(move || {
            for path in doomed {
                let _ = files.discard(&[path]);
            }
            drop(replies);
        }));
    }
}

/// A copy below the limit of the table also lies in the directory (or in the cache without one): the bytes of the steps come from one place.
fn keep_small_copy(files: &FileDirectories, id: &str, location: crate::store::files::Location, data: &[u8]) -> std::io::Result<crate::store::files::Location> {
    if location == crate::store::files::Location::Directory { return Ok(location); }
    if files.has_directory() {
        files.write(id, data)?;
        Ok(crate::store::files::Location::Directory)
    } else {
        files.materialize(id, "source", data)?;
        Ok(crate::store::files::Location::Table)
    }
}

/// The outcome of the abort: success is `Ok`, any other answer is the fault with its HTTP status and class.
pub(super) fn abort_result(result: &Result<(), TransportError>) -> Result<(), AbortFault> {
    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(AbortFault { temporary: temporary(error), status: if let TransportError::Http { status, .. } = error { Some(*status) } else { None } }),
    }
}

pub(crate) fn temporary(error: &TransportError) -> bool { RetryClass::of_error(error) != RetryClass::Fatal }

impl EffectRunner<Transfers> for TransferExecutor {
    fn step_done(&mut self) { self.finish_files(); }

    fn run(&mut self, stamp: EpochStamp, effect: TransferEffect, back: &Back<Transfers>) {
        match effect {
            TransferEffect::Copy { transfer_id, attempt_id, source, staged, .. } => self.copy(stamp, back, (transfer_id, attempt_id), source, staged),
            TransferEffect::PromoteStaged { transfer_id } => {
                match self.files.promote(&staged_file_id(transfer_id), &file_id(transfer_id)) {
                    Ok(_) => back.reply(stamp, TransferInput::StagedPromoted { transfer_id }),
                    Err(error) => back.reply(stamp, TransferInput::StagedPromoteFailed { transfer_id, detail: error.to_string() }),
                }
            }
            TransferEffect::CopyCancel { transfer_id } => self.copies.cancel(&transfer_id),
            TransferEffect::Probe { transfer_id, attempt_id, kind } => self.probe(stamp, back, (transfer_id, attempt_id), kind),
            TransferEffect::Prepare { transfer_id, attempt_id, plan } => self.prepare(stamp, back, (transfer_id, attempt_id), plan),
            TransferEffect::PrepareCancel { transfer_id, attempt_id } => self.prepare_cancel(transfer_id, attempt_id),
            TransferEffect::Measure { transfer_id, attempt_id } => self.measure(stamp, back, (transfer_id, attempt_id)),
            TransferEffect::Step { transfer_id, attempt_id, cycle_id, key, operation, part, purpose, upload_id, source, steps, seeded, .. } => {
                debug_assert_eq!(purpose, NetworkPurpose::Transfer);
                self.step(stamp, back, (transfer_id, attempt_id), StepAsk { cycle_id, operation, part, upload_id, source, steps, key, seeded });
            }
            TransferEffect::Complete { transfer_id, attempt_id, cycle_id, key, upload_id, parts, seeded, .. } => self.complete(stamp, back, (transfer_id, attempt_id, cycle_id), key, (upload_id, parts), seeded),
            TransferEffect::Abort { upload_id, .. } => self.drop_upload(upload_id),
            TransferEffect::AbortOrphan { upload_id, .. } => self.abort_orphan(stamp, back, upload_id),
            TransferEffect::CancelStep { transfer_id } => self.steps.cancel(&transfer_id),
            TransferEffect::ReleaseStep { request_id } => (self.signal)(LifeSignal::ReleaseStep(request_id)),
            TransferEffect::DeleteFile { path } => self.doomed.push(path),
            TransferEffect::Reply { reply, answer } => self.answers.push((reply, answer)),
            TransferEffect::Status { stream_id, shown, revision, .. } => (self.status)(stream_id, shown, revision),
            TransferEffect::Started { epoch } => (self.signal)(LifeSignal::Started(epoch)),
            TransferEffect::SnapshotRequest { epoch } => (self.signal)(LifeSignal::SnapshotRequest(epoch)),
            TransferEffect::RestoreClassified { epoch } => (self.signal)(LifeSignal::RestoreClassified(epoch)),
            TransferEffect::Drained(drained) => (self.drained)(drained),
        }
    }
}
