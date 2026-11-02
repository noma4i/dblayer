use serde_json::{json, Map};

use crate::db::Row;
use crate::identity::RowId;
use crate::schema::{Coverage, NodeRead, RemoteShape, NODE_PARAM};
use crate::transport::GraphQLRequest;

use super::error::SyncError;
use super::extract::extract;
use super::land::Landed;
use super::load::Resolved;
use super::normalize::normalize_nodes;
use std::sync::Arc;

use super::{Inner, Sync};

impl Sync {
    pub async fn node(&self, model: &str, relation: &str, id: &str, force: bool) -> Result<Option<Row>, SyncError> {
        self.inner.node(model, relation, id, force).await
    }

    pub async fn nodes(&self, model: &str, relation: &str, ids: &[&str]) -> Result<Vec<Row>, SyncError> {
        let _app = self.app_load();
        let resolved = self.inner.resolve_node(model, relation, NodeRead::Many, Map::new())?;
        let attempt = self.inner.begin_attempt(&resolved.window_key)?;
        let request = GraphQLRequest::new(&resolved.operation.document, &resolved.operation.name, json!({ "ids": ids }));
        let (response, _) = self.inner.query_with_retry(request).await?;
        let data = response.data.ok_or_else(|| SyncError::Malformed("response has no data".to_string()))?;
        let root = resolved.remote.root.as_deref().unwrap_or(&resolved.operation.root_path);
        let extracted = extract(&data, root, &RemoteShape::List { coverage: Coverage::Complete }, None)?;
        let normalized = normalize_nodes(&self.inner.db, model, &extracted.nodes)?;
        if let Landed::Discarded(_) = self.inner.land_rows(model, &normalized, &attempt)? {
            return Ok(Vec::new());
        }
        let mut rows: Vec<Row> = Vec::new();
        for id in &normalized.order {
            rows.extend(self.inner.db.find(model, id)?);
        }
        Ok(rows)
    }
}

impl Inner {
    pub(crate) async fn node(self: &Arc<Self>, model: &str, relation: &str, id: &str, force: bool) -> Result<Option<Row>, SyncError> {
        let _app = self.app_load();
        let row_id = RowId::new(id).map_err(|error| SyncError::Params(error.to_string()))?;
        let resolved = self.resolve_node(model, relation, NodeRead::One, Map::from_iter([(NODE_PARAM.to_string(), json!(id))]))?;
        self.fetch(&resolved, None, force).await?;
        let listed = self.db.shared.store.read(&[model], |reader| reader.scope_has_member(model, resolved.scope.key.as_str(), id))?;
        Ok(if listed { self.db.find(model, &row_id)? } else { None })
    }

    fn resolve_node(&self, model: &str, relation: &str, kind: NodeRead, params: Map<String, serde_json::Value>) -> Result<Resolved<'_>, SyncError> {
        let spec = self.db.spec(model).map_err(|_| SyncError::UnknownModel(model.to_string()))?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation);
        if declared.is_some_and(|declared| declared.node != Some(kind)) {
            return Err(SyncError::Params(format!("relation {relation} of {model} is not a {kind:?} node read")));
        }
        self.resolve(model, relation, &params)
    }
}
