//! The codec of the file steps of Transfer: the requests of `createBlob` and of the multipart upload, and the answers.
//! It is stateless: the blob key and the multipart id travel in `upload_id` of the owner, the names of the operations come from `UploadOperations`.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::transfer::{Source, Steps};
use super::transfer_executor::{Link, LinkPut, StepCodec, StepDone, StepEnd};
use crate::owners::life::OperationLookup;
use crate::owners::read::codec::root_value;
use crate::transport::{CancelToken, GraphQLRequest, PutFile, PutRequest, RequestOwner, Response, TransferIdentity, TransferStep, TransportError};
use crate::owners::write::upload::{parse_headers, part_range, UploadConfig, UploadOperations, UploadStage};
use crate::owners::write::upload_io::md5_base64;

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

struct Answered {
    data: Value,
    label: String,
}

impl Answered {
    fn text(&self, field: &str) -> Result<String, TransportError> {
        self.data.get(field).and_then(Value::as_str).map(str::to_string).ok_or_else(|| TransportError::Decode(format!("{} has no {field}", self.label)))
    }
}

pub(crate) struct UploadCodec {
    operations: UploadOperations,
    lookup: OperationLookup,
    config: UploadConfig,
}

impl UploadCodec {
    pub fn new(operations: UploadOperations, lookup: OperationLookup, config: UploadConfig) -> Self { Self { operations, lookup, config } }

    async fn graphql(&self, link: &Link, (name, step): (&str, UploadStage), (operation, item): (u64, &str), variables: Value) -> Result<Answered, TransportError> {
        let (document, registered) = (self.lookup)(name).ok_or_else(|| rejected(&format!("operation {name} is not registered")))?;
        let root = self.operations.root_of(name).map_or(registered, str::to_string);
        let key = if step == UploadStage::Abort { RequestOwner::Orphan { upload_id: item.to_string() } } else { RequestOwner::Transfer { operation_id: i64::try_from(operation).unwrap_or(i64::MAX), item_id: item.to_string(), step } };
        let request = GraphQLRequest { owner: Some(key.clone()), ..GraphQLRequest::new(&document, name, variables) };
        let response = link.mutate(request, &key).await?;
        let data = if step == UploadStage::Abort {
            if !response.errors.is_empty() { return Err(TransportError::GraphqlAnswer { response: Box::new(response) }); }
            Value::Null
        } else { answered(name, &root, response)? };
        Ok(Answered { data, label: format!("{name}: {root}") })
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
        put_status(link.put(request).await?)
    }

    async fn single(&self, link: &Link, operation: u64, body: &StepsBody, source: &Source, total: u64) -> Result<StepDone, TransportError> {
        let create = self.operations.create_blob.as_ref().ok_or_else(|| rejected("the upload has no createBlob operation"))?;
        let checksum = md5_base64(Path::new(&source.path), &CancelToken::new()).await.map_err(unreadable)?;
        let variables = json!({"filename": body.name, "contentType": body.mime, "byteSize": total, "checksum": checksum});
        let data = self.graphql(link, (&create.name, UploadStage::Create), (operation, &body.item), variables).await?;
        let (url, signed) = (data.text("url")?, data.text("signedId")?);
        let headers = parse_headers(data.data.get("headers")).map_err(TransportError::Decode)?;
        let file = PutFile { path: source.path.clone(), offset: 0, length: total };
        self.put(link, (&url, headers), file, (TransferStep::Put, (operation, &body.item))).await?;
        Ok(StepDone { upload_id: Some(Held::Blob { signed_id: signed }.text()), etag: None, ..Default::default() })
    }

    async fn part(&self, link: &Link, operation: u64, part: u32, held: Option<&str>, (body, source, total): (&StepsBody, &Source, u64)) -> Result<StepDone, TransportError> {
        let steps = self.operations.multipart.as_ref().ok_or_else(|| rejected("the upload has no multipart operations"))?;
        let (blob, minted) = match held {
            Some(text) => (Held::read(Some(text))?, None),
            None => {
                let checksum = md5_base64(Path::new(&source.path), &CancelToken::new()).await.map_err(unreadable)?;
                let variables = json!({"filename": body.name, "contentType": body.mime, "byteSize": total, "checksum": checksum});
                let data = self.graphql(link, (&steps.initiate.name, UploadStage::Init), (operation, &body.item), variables).await?;
                let created = Held::Multipart { key: data.text("key")?, id: data.text("uploadId")? };
                link.minted(created.text());
                (created.clone(), Some(created.text()))
            }
        };
        if matches!(link.put, LinkPut::InitOnly) {
            return Ok(StepDone { upload_id: minted, etag: None, end: StepEnd::Initiated });
        }
        let Held::Multipart { key: blob_key, id } = blob else { return Err(rejected("the upload id is not a multipart id")) };
        let data = self.graphql(link, (&steps.presign.name, UploadStage::Presign), (operation, &body.item), json!({"key": blob_key, "uploadId": id, "partNumbers": [part]})).await?;
        let target = data.data.get("parts").and_then(Value::as_array).into_iter().flatten().find(|item| item.get("partNumber").and_then(Value::as_u64) == Some(u64::from(part))).ok_or_else(|| TransportError::Decode("the presign answer misses the part".into()))?;
        let url = target.get("url").and_then(Value::as_str).ok_or_else(|| TransportError::Decode("the presign answer has no url".into()))?;
        let headers = parse_headers(target.get("headers")).map_err(TransportError::Decode)?;
        if matches!(link.put, LinkPut::Detached { .. }) {
            let request = PutRequest { url: url.to_string(), headers, file: PutFile { path: source.path.clone(), offset: 0, length: total }, identity: TransferIdentity { operation_id: i64::try_from(operation).unwrap_or(i64::MAX), item_id: body.item.clone(), step: TransferStep::Part { number: part } }, cancel: CancelToken::new(), sent: Arc::new(|_| {}) };
            link.start_put(request)?;
            return Ok(StepDone { upload_id: minted, etag: None, end: StepEnd::Started });
        }
        let (offset, length) = part_range(total, self.config.part_size, part);
        let etag = self.put(link, (url, headers), PutFile { path: source.path.clone(), offset, length }, (TransferStep::Part { number: part }, (operation, &body.item))).await?;
        let etag = etag.ok_or_else(|| TransportError::Decode("the part answer has no etag".into()))?;
        Ok(StepDone { upload_id: minted, etag: Some(etag), ..Default::default() })
    }
}

/// The answer of a PUT: the etag of a 2xx, the error of any other status. One function for the PUT in the process and the one outside it.
pub(crate) fn put_status(reply: crate::transport::PutReply) -> Result<Option<String>, TransportError> {
    match reply.status {
        200..=299 => Ok(reply.etag),
        403 => Err(TransportError::Network("the upload url expired".into())),
        status => Err(TransportError::Http { status, body: String::new() }),
    }
}

fn rejected(message: &str) -> TransportError { TransportError::Graphql { messages: vec![message.to_string()], extensions: None } }

fn unreadable(error: std::io::Error) -> TransportError { TransportError::Graphql { messages: vec![format!("source file is unreadable: {error}")], extensions: None } }

/// The root of a committed answer, or the error of the server as a transport error: the class of the error decides a repeat.
fn answered(name: &str, root: &str, response: Response) -> Result<Value, TransportError> {
    if !response.errors.is_empty() {
        return Err(TransportError::GraphqlAnswer { response: Box::new(response) });
    }
    response.data.and_then(|data| root_value(&data, root).cloned()).filter(Value::is_object).ok_or_else(|| TransportError::Decode(format!("{name} answered without object at {root}")))
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
                Ok(Some(data.text("signedId")?))
            }
        }
    }

    async fn discard_upload(&self, link: &Link, upload_id: Option<&str>) -> Result<(), TransportError> {
        let Held::Multipart { key, id } = Held::read(upload_id)? else { return Ok(()) };
        let steps = self.operations.multipart.as_ref().ok_or_else(|| rejected("the upload has no multipart operations"))?;
        self.graphql(link, (&steps.abort.name, UploadStage::Abort), (0, upload_id.unwrap_or_default()), json!({"key": key, "uploadId": id})).await.map(|_| ())
    }
}
