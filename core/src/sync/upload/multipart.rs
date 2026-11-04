use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use crate::task::Children;

use crate::syncstate::{FailureKind, OperationFailure};
use crate::upload::{md5_base64, parse_headers, part_count, part_range, UploadEvent, UploadPart, UploadStage};

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
            .ok_or_else(|| Stop::failed(OperationFailure::new(FailureKind::Rejected, "the upload has no multipart steps")))?;
        let config = &self.inner.config.upload;
        if self.snapshot().upload_id.is_none() {
            self.save(UploadEvent::InitStarted)?;
            let checksum = md5_base64(std::path::Path::new(&self.input.path), &self.token).await
                .map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Rejected, format!("source file is unreadable: {error}"))))?;
            let variables = json!({"filename": self.file_name(), "contentType": self.content_type(), "byteSize": total, "checksum": checksum});
            let data = self.retrying(UploadStage::Init, 0, || self.graphql(&steps.init, variables.clone(), true)).await?;
            let (key, upload_id) = (text(&data, "key").map_err(stop_of)?, text(&data, "uploadId").map_err(stop_of)?);
            self.save(UploadEvent::Initiated { key, upload_id })?;
        }
        let stored = self.snapshot();
        let (Some(key), Some(upload_id)) = (stored.key.clone(), stored.upload_id.clone()) else {
            return Err(Stop::failed(OperationFailure::new(FailureKind::Unrecognized, "the multipart upload has no id")));
        };
        let finished: BTreeMap<u32, String> = stored.parts.iter().map(|part| (part.number, part.etag.clone())).collect();
        let size = |number: u32| part_range(total, config.part_size, number).1;
        self.progress().begin_phase(finished.keys().map(|number| (*number, size(*number))));
        let baseline = self.progress().sent();
        self.save(UploadEvent::Baseline { sent: baseline })?;
        let mut queue: VecDeque<u32> = (1..=part_count(total, config.part_size)).filter(|number| !finished.contains_key(number)).collect();
        let mut targets: HashMap<u32, PartTarget> = HashMap::new();
        let mut presigned_at = Duration::ZERO;
        let mut flight: Children<PartOutcome> = Children::new();
        let limit = config.parallel_parts.max(1);
        loop {
            let aged = self.inner.clock.now().saturating_sub(presigned_at) > config.presign_ttl;
            if !queue.is_empty() && (targets.is_empty() || aged) {
                let numbers: Vec<u32> = self.snapshot().request.filter(|request| request.name == steps.presign && super::super::locked(&self.resumed).is_some())
                    .map(|request| serde_json::from_value(request.variables["partNumbers"].clone())).transpose()
                    .map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?
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
                flight.spawn(crate::task::Carry::here(), async move {
                    let outcome = runner.retrying(UploadStage::Part, number, || runner.put_once(&url, &headers, offset, length, number, crate::transport::TransferStep::Part { number })).await;
                    (number, outcome)
                });
            }
            let Some(joined) = flight.join_next().await else { break };
            let (number, outcome) = joined.map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
            match outcome {
                Ok(Some(etag)) => {
                    self.progress().finish(number, size(number));
                    let sent = self.progress().sent();
                    self.save(UploadEvent::PartConfirmed { part: UploadPart { number, etag, confirmed_bytes: size(number) }, sent })?;
                }
                Ok(None) => return self.stop_flight(flight, Stop::failed(OperationFailure::new(FailureKind::Unrecognized, "the part answer has no etag"))).await,
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
        self.save(UploadEvent::CompleteStarted)?;
        let parts: Vec<Value> = self.snapshot().parts.iter().map(|part| json!({"partNumber": part.number, "etag": part.etag})).collect();
        let variables = json!({"key": key, "uploadId": upload_id, "parts": parts});
        let data = self.retrying(UploadStage::Complete, 0, || self.graphql(&steps.complete, variables.clone(), true)).await?;
        let (signed, blob) = (text(&data, "signedId").map_err(stop_of)?, text(&data, "blobId").map_err(stop_of)?);
        self.save(UploadEvent::Completed { signed_id: signed, blob_id: blob, total })
    }

    async fn presign(self: &Arc<Self>, operation: &str, key: &str, upload_id: &str, numbers: &[u32]) -> Result<HashMap<u32, PartTarget>, Stop> {
        self.save(UploadEvent::PresignStarted)?;
        let variables = json!({"key": key, "uploadId": upload_id, "partNumbers": numbers});
        let data = self.retrying(UploadStage::Presign, 0, || self.graphql(operation, variables.clone(), true)).await?;
        let mut targets = HashMap::new();
        let parts = data.get("parts").and_then(Value::as_array).ok_or_else(|| stop_of(fatal(FailureKind::Unrecognized, "the presign answer has no parts list")))?;
        for item in parts {
            let number = item.get("partNumber").and_then(Value::as_u64).and_then(|number| u32::try_from(number).ok()).filter(|number| *number > 0);
            let (Some(number), Some(url)) = (number, item.get("url").and_then(Value::as_str)) else { return Err(stop_of(fatal(FailureKind::Unrecognized, "the presign answer has an invalid part"))); };
            let headers = parse_headers(item.get("headers")).map_err(|error| stop_of(fatal(FailureKind::Unrecognized, error)))?;
            if targets.insert(number, (url.to_string(), headers)).is_some() { return Err(stop_of(fatal(FailureKind::Unrecognized, "the presign answer repeats a part"))); }
        }
        if numbers.iter().any(|number| !targets.contains_key(number)) {
            return Err(stop_of(fatal(FailureKind::Unrecognized, "the presign answer misses requested parts")));
        }
        self.save(UploadEvent::Presigned)?;
        Ok(targets)
    }

    async fn stop_flight(&self, mut flight: Children<PartOutcome>, stop: Stop) -> Result<(), Stop> {
        flight.abort_all();
        while flight.join_next().await.is_some() {}
        Err(stop)
    }
}
