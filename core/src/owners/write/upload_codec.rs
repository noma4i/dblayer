//! The codec of the file steps of Transfer: the requests of `createBlob` and of the multipart upload, and the answers.
//! It is stateless: the blob key and the multipart id travel in `upload_id` of the owner, the names of the operations come from `UploadOperations`.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::transfer::{Source, Steps};
use super::transfer_executor::{Link, StepCodec, StepDone};
use crate::owners::life::OperationLookup;
use crate::owners::read::codec::root_value;
use crate::transport::{CancelToken, GraphQLRequest, PutFile, PutRequest, RequestOwner, Response, TransferIdentity, TransferStep, TransportError};
use crate::owners::write::upload::{md5_base64, parse_headers, part_range, UploadConfig, UploadOperations, UploadStage};

/// What `Steps.body` holds: the file the steps are for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepsBody {
    pub multipart: bool,
    pub name: String,
    pub mime: String,
    pub signed_variable: String, pub item: String,
}

impl StepsBody {
    pub fn steps(&self) -> Steps { Steps { path: String::new(), body: serde_json::to_string(self).unwrap_or_default() } }

    fn of(steps: &Steps) -> Result<Self, TransportError> { serde_json::from_str(&steps.body).map_err(|error| TransportError::Decode(format!("the steps of the upload: {error}"))) }
}

/// What the owner keeps as `upload_id`: the id of a multipart upload with the key of its blob, or the signed id of a blob that a single PUT filled.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Held {
    Multipart { key: String, id: String },
    Blob { signed_id: String },
}

impl Held {
    fn read(text: Option<&str>) -> Result<Self, TransportError> {
        let text = text.ok_or_else(|| TransportError::Decode("the upload has no id".into()))?;
        serde_json::from_str(text).map_err(|error| TransportError::Decode(format!("the upload id: {error}")))
    }

    fn text(&self) -> String { serde_json::to_string(self).unwrap_or_default() }
}

pub(crate) struct UploadCodec {
    operations: UploadOperations,
    lookup: OperationLookup,
    config: UploadConfig,
}

impl UploadCodec {
    pub fn new(operations: UploadOperations, lookup: OperationLookup, config: UploadConfig) -> Self { Self { operations, lookup, config } }

    async fn graphql(&self, link: &Link, (name, step): (&str, UploadStage), (operation, item): (u64, &str), variables: Value) -> Result<Value, TransportError> {
        let (document, root) = (self.lookup)(name).ok_or_else(|| rejected(&format!("operation {name} is not registered")))?;
        let response = link.mutate(GraphQLRequest { owner: Some(RequestOwner::Transfer { operation_id: i64::try_from(operation).unwrap_or(i64::MAX), item_id: item.to_string(), step }), ..GraphQLRequest::new(&document, name, variables) }).await?;
        answered(name, &root, response)
    }

    async fn put(&self, link: &Link, (url, headers): (&str, Vec<(String, String)>), file: PutFile, (step, identity): (TransferStep, (u64, &str))) -> Result<Option<String>, TransportError> {
        let request = PutRequest {
            url: url.to_string(),
            headers,
            file,
            identity: TransferIdentity { operation_id: i64::try_from(identity.0).unwrap_or(i64::MAX), item_id: identity.1.to_string(), step },
            cancel: CancelToken::new(),
            sent: Arc::new(|_| {}),
        };
        let response = tokio::time::timeout(self.config.put_timeout, link.put(request)).await.map_err(|_| TransportError::Network("put timed out".into()))??;
        match response.status {
            200..=299 => Ok(response.etag),
            403 => Err(TransportError::Network("the upload url expired".into())),
            status => Err(TransportError::Http { status, body: String::new() }),
        }
    }

    async fn single(&self, link: &Link, operation: u64, body: &StepsBody, source: &Source, total: u64) -> Result<StepDone, TransportError> {
        let create = self.operations.create_blob.as_ref().ok_or_else(|| rejected("the upload has no createBlob operation"))?;
        let checksum = md5_base64(Path::new(&source.path), &CancelToken::new()).await.map_err(unreadable)?;
        let variables = json!({"filename": body.name, "contentType": body.mime, "byteSize": total, "checksum": checksum});
        let data = self.graphql(link, (&create.name, UploadStage::Create), (operation, &body.item), variables).await?;
        let (url, signed) = (text(&data, "url")?, text(&data, "signedId")?);
        let headers = parse_headers(data.get("headers")).map_err(TransportError::Decode)?;
        let file = PutFile { path: source.path.clone(), offset: 0, length: total };
        self.put(link, (&url, headers), file, (TransferStep::Put, (operation, &body.item))).await?;
        Ok(StepDone { upload_id: Some(Held::Blob { signed_id: signed }.text()), etag: None })
    }

    async fn part(&self, link: &Link, operation: u64, part: u32, held: Option<&str>, (body, source, total): (&StepsBody, &Source, u64)) -> Result<StepDone, TransportError> {
        let steps = self.operations.multipart.as_ref().ok_or_else(|| rejected("the upload has no multipart operations"))?;
        let (blob, minted) = match held {
            Some(text) => (Held::read(Some(text))?, None),
            None => {
                let checksum = md5_base64(Path::new(&source.path), &CancelToken::new()).await.map_err(unreadable)?;
                let variables = json!({"filename": body.name, "contentType": body.mime, "byteSize": total, "checksum": checksum});
                let data = self.graphql(link, (&steps.initiate.name, UploadStage::Init), (operation, &body.item), variables).await?;
                let created = Held::Multipart { key: text(&data, "key")?, id: text(&data, "uploadId")? };
                link.minted(created.text());
                (created.clone(), Some(created.text()))
            }
        };
        let Held::Multipart { key: blob_key, id } = blob else { return Err(rejected("the upload id is not a multipart id")) };
        let data = self.graphql(link, (&steps.presign.name, UploadStage::Presign), (operation, &body.item), json!({"key": blob_key, "uploadId": id, "partNumbers": [part]})).await?;
        let target = data.get("parts").and_then(Value::as_array).into_iter().flatten().find(|item| item.get("partNumber").and_then(Value::as_u64) == Some(u64::from(part))).ok_or_else(|| TransportError::Decode("the presign answer misses the part".into()))?;
        let url = target.get("url").and_then(Value::as_str).ok_or_else(|| TransportError::Decode("the presign answer has no url".into()))?;
        let headers = parse_headers(target.get("headers")).map_err(TransportError::Decode)?;
        let (offset, length) = part_range(total, self.config.part_size, part);
        let etag = self.put(link, (url, headers), PutFile { path: source.path.clone(), offset, length }, (TransferStep::Part { number: part }, (operation, &body.item))).await?;
        let etag = etag.ok_or_else(|| TransportError::Decode("the part answer has no etag".into()))?;
        Ok(StepDone { upload_id: minted, etag: Some(etag) })
    }
}

fn rejected(message: &str) -> TransportError { TransportError::Graphql { messages: vec![message.to_string()], extensions: None } }

fn unreadable(error: std::io::Error) -> TransportError { TransportError::Graphql { messages: vec![format!("source file is unreadable: {error}")], extensions: None } }

fn text(data: &Value, field: &str) -> Result<String, TransportError> {
    data.get(field).and_then(Value::as_str).map(str::to_string).ok_or_else(|| TransportError::Decode(format!("the answer has no {field}")))
}

/// The root of a committed answer, or the error of the server as a transport error: the class of the error decides a repeat.
fn answered(name: &str, root: &str, response: Response) -> Result<Value, TransportError> {
    if !response.errors.is_empty() {
        let extensions = response.extensions();
        return Err(TransportError::Graphql { messages: response.errors.iter().map(|error| error.message.clone()).collect(), extensions });
    }
    response.data.and_then(|data| root_value(&data, root).cloned()).filter(|value| !value.is_null()).ok_or_else(|| TransportError::Decode(format!("{name} answered without {root}")))
}

#[async_trait]
impl StepCodec for UploadCodec {
    async fn step(&self, link: &Link, operation: u64, part: Option<u32>, upload_id: Option<&str>, source: &Source, steps: &Steps) -> Result<StepDone, TransportError> {
        let body = StepsBody::of(steps)?;
        let total = std::fs::metadata(&source.path).map_err(unreadable)?.len();
        match (body.multipart, part) {
            (false, _) => self.single(link, operation, &body, source, total).await,
            (true, Some(part)) => self.part(link, operation, part, upload_id, (&body, source, total)).await,
            (true, None) => Err(rejected("a multipart step needs a part")),
        }
    }

    async fn complete(&self, link: &Link, transfer_key: &str, upload_id: Option<&str>, parts: &[(u32, Option<String>)]) -> Result<Option<String>, TransportError> {
        match Held::read(upload_id)? {
            Held::Blob { signed_id } => Ok(Some(signed_id)),
            Held::Multipart { key, id } => {
                let steps = self.operations.multipart.as_ref().ok_or_else(|| rejected("the upload has no multipart operations"))?;
                let listed: Vec<Value> = parts.iter().map(|(number, etag)| json!({"partNumber": number, "etag": etag})).collect();
                let data = self.graphql(link, (&steps.complete.name, UploadStage::Complete), (0, transfer_key), json!({"key": key, "uploadId": id, "parts": listed})).await?;
                Ok(Some(text(&data, "signedId")?))
            }
        }
    }

    async fn discard_upload(&self, link: &Link, transfer_key: &str, upload_id: Option<&str>) -> Result<(), TransportError> {
        let Held::Multipart { key, id } = Held::read(upload_id)? else { return Ok(()) };
        let steps = self.operations.multipart.as_ref().ok_or_else(|| rejected("the upload has no multipart operations"))?;
        self.graphql(link, (&steps.abort.name, UploadStage::Abort), (0, transfer_key), json!({"key": key, "uploadId": id})).await.map(|_| ())
    }
}
