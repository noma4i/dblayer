use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::task::JoinSet;

use crate::syncstate::{FailureKind, OperationFailure};
use crate::upload::{md5_base64, parse_headers, part_count, part_range, UploadPart, UploadStage};

use super::run::{stop_of, Runner};
use super::steps::{fatal, text, Stop, EXPIRED_MESSAGE};

type PartTarget = (String, Vec<(String, String)>);
type PartOutcome = (u32, Result<Option<String>, Stop>);

impl Runner {
    pub(super) async fn multipart(self: &Arc<Self>, total: u64) -> Result<(), Stop> {
        let steps = self
            .input
            .steps
            .multipart
            .clone()
            .ok_or_else(|| Stop::Failed(OperationFailure::new(FailureKind::Rejected, "the upload has no multipart steps")))?;
        let config = &self.inner.config.upload;
        if self.snapshot().upload_id.is_none() {
            self.save(|state| state.stage = UploadStage::Init)?;
            let checksum = md5_base64(std::path::Path::new(&self.input.path))
                .map_err(|error| Stop::Failed(OperationFailure::new(FailureKind::Rejected, format!("source file is unreadable: {error}"))))?;
            let variables = json!({"filename": self.file_name(), "contentType": self.content_type(), "byteSize": total, "checksum": checksum});
            let data = self.retrying(UploadStage::Init, 0, || self.graphql(&steps.init, variables.clone(), true)).await?;
            let (key, upload_id) = (text(&data, "key").map_err(stop_of)?, text(&data, "uploadId").map_err(stop_of)?);
            self.save(|state| {
                state.key = Some(key);
                state.upload_id = Some(upload_id);
                state.parts.clear();
                state.sent_bytes = 0;
            })?;
        }
        let stored = self.snapshot();
        let (Some(key), Some(upload_id)) = (stored.key.clone(), stored.upload_id.clone()) else {
            return Err(Stop::Failed(OperationFailure::new(FailureKind::Unrecognized, "the multipart upload has no id")));
        };
        let finished: BTreeMap<u32, String> = stored.parts.iter().map(|part| (part.number, part.etag.clone())).collect();
        let size = |number: u32| part_range(total, config.part_size, number).1;
        self.progress().begin_phase(finished.keys().map(|number| (*number, size(*number))));
        let baseline = self.progress().sent();
        self.save(|state| state.sent_bytes = baseline)?;
        let mut queue: VecDeque<u32> = (1..=part_count(total, config.part_size)).filter(|number| !finished.contains_key(number)).collect();
        let mut targets: HashMap<u32, PartTarget> = HashMap::new();
        let mut presigned_at = Duration::ZERO;
        let mut flight: JoinSet<PartOutcome> = JoinSet::new();
        let limit = config.parallel_parts.max(1);
        loop {
            let aged = self.inner.clock.now().saturating_sub(presigned_at) > config.presign_ttl;
            if !queue.is_empty() && (targets.is_empty() || aged) {
                let numbers: Vec<u32> = self.snapshot().request.filter(|request| request.name == steps.presign && super::super::locked(&self.resumed).is_some())
                    .and_then(|request| serde_json::from_value(request.variables["partNumbers"].clone()).ok())
                    .unwrap_or_else(|| queue.iter().copied().collect());
                targets.extend(self.presign(&steps.presign, &key, &upload_id, &numbers).await?);
                presigned_at = self.inner.clock.now();
            }
            while flight.len() < limit {
                let Some(position) = queue.iter().position(|number| targets.contains_key(number)) else { break };
                let Some(number) = queue.remove(position) else { break };
                let Some((url, headers)) = targets.remove(&number) else { break };
                let (offset, length) = part_range(total, config.part_size, number);
                let runner = self.clone();
                flight.spawn(async move {
                    let outcome = runner.retrying(UploadStage::Part, number, || runner.put_once(&url, &headers, offset, length, number)).await;
                    (number, outcome)
                });
            }
            let Some(joined) = flight.join_next().await else { break };
            let (number, outcome) = joined.map_err(|error| Stop::Failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
            match outcome {
                Ok(Some(etag)) => {
                    self.progress().finish(number, size(number));
                    let sent = self.progress().sent();
                    self.save(|state| {
                        state.parts.push(UploadPart { number, etag, confirmed_bytes: size(number) });
                        state.parts.sort_by_key(|part| part.number);
                        state.sent_bytes = state.sent_bytes.max(sent);
                        state.confirmed_bytes = sent;
                    })?;
                }
                Ok(None) => return self.stop_flight(flight, Stop::Failed(OperationFailure::new(FailureKind::Unrecognized, "the part answer has no etag"))).await,
                Err(Stop::Expired) => {
                    if let Err(stop) = self.pause_retry(UploadStage::Part, number, OperationFailure::new(FailureKind::Rejected, EXPIRED_MESSAGE)).await {
                        return self.stop_flight(flight, stop).await;
                    }
                    match self.presign(&steps.presign, &key, &upload_id, &[number]).await {
                        Ok(fresh) => targets.extend(fresh),
                        Err(stop) => return self.stop_flight(flight, stop).await,
                    }
                    queue.push_front(number);
                }
                Err(stop) => return self.stop_flight(flight, stop).await,
            }
        }
        self.save(|state| state.stage = UploadStage::Complete)?;
        let parts: Vec<Value> = self.snapshot().parts.iter().map(|part| json!({"partNumber": part.number, "etag": part.etag})).collect();
        let variables = json!({"key": key, "uploadId": upload_id, "parts": parts});
        let data = self.retrying(UploadStage::Complete, 0, || self.graphql(&steps.complete, variables.clone(), true)).await?;
        let (signed, blob) = (text(&data, "signedId").map_err(stop_of)?, text(&data, "blobId").map_err(stop_of)?);
        self.save(|state| {
            state.signed_id = Some(signed);
            state.blob_id = Some(blob);
            state.sent_bytes = total;
            state.confirmed_bytes = total;
            state.transferred = true;
            state.key = None;
            state.upload_id = None;
            state.parts.clear();
        })
    }

    async fn presign(self: &Arc<Self>, operation: &str, key: &str, upload_id: &str, numbers: &[u32]) -> Result<HashMap<u32, PartTarget>, Stop> {
        self.save(|state| state.stage = UploadStage::Presign)?;
        let variables = json!({"key": key, "uploadId": upload_id, "partNumbers": numbers});
        let data = self.retrying(UploadStage::Presign, 0, || self.graphql(operation, variables.clone(), true)).await?;
        let mut targets = HashMap::new();
        for item in data.get("parts").and_then(Value::as_array).into_iter().flatten() {
            let (Some(number), Some(url)) = (item.get("partNumber").and_then(Value::as_u64), item.get("url").and_then(Value::as_str)) else { continue };
            targets.insert(number as u32, (url.to_string(), parse_headers(item.get("headers"))));
        }
        if numbers.iter().any(|number| !targets.contains_key(number)) {
            return Err(stop_of(fatal(FailureKind::Unrecognized, "the presign answer misses requested parts")));
        }
        self.save(|state| state.stage = UploadStage::Part)?;
        Ok(targets)
    }

    async fn stop_flight(&self, mut flight: JoinSet<PartOutcome>, stop: Stop) -> Result<(), Stop> {
        flight.abort_all();
        while flight.join_next().await.is_some() {}
        Err(stop)
    }
}
