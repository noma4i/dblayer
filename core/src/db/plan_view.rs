//! The base as the plan of the optimistic effects reads it.

use serde_json::{Map, Value};

use crate::identity::{ModelKey, RowId, RowRef};
use crate::owners::write::outbox_plan::{PlanError, PlanView};
use crate::schema::ModelSpec;
use crate::store::Reader;

use super::open::Db;

pub(crate) struct ReaderView<'a, 'r> {
    pub reader: &'a Reader<'r>,
    pub specs: &'a dyn Fn(&str) -> Option<&'a ModelSpec>,
    /// Needed for the rows that go with a removed row; the rebuild of open effects has none.
    pub db: Option<&'a Db>,
}

fn fault(error: impl std::fmt::Display) -> PlanError { PlanError(error.to_string()) }

impl PlanView for ReaderView<'_, '_> {
    fn row(&self, model: &str, id: &str) -> Result<Option<Map<String, Value>>, PlanError> { self.reader.point(model, id).map_err(fault) }

    fn member_key(&self, model: &str, list: &str, id: &str) -> Result<Option<String>, PlanError> { self.reader.scope_member_key(model, list, id).map_err(fault) }

    fn last_key(&self, model: &str, list: &str) -> Result<Option<String>, PlanError> { self.reader.scope_last_key(model, list).map_err(fault) }

    fn singleton(&self, model: &str) -> Result<Option<String>, PlanError> {
        let spec = (self.specs)(model).ok_or_else(|| PlanError(format!("unknown model {model}")))?;
        let Some(relation) = spec.singleton_relation() else { return Ok(None) };
        let key = spec.selection_key(&relation.name, &Map::new()).map_err(fault)?;
        Ok(self.reader.scope_members(model, key.as_str()).map_err(fault)?.into_iter().next().map(|(id, _)| id))
    }

    fn removed_with(&self, model: &str, id: &str) -> Result<Vec<(String, String)>, PlanError> {
        let db = self.db.ok_or_else(|| PlanError("the rows of a removal need the database".into()))?;
        let own = RowRef::new(ModelKey::new(model), RowId::new(id).map_err(fault)?);
        Ok(db.destroy_targets(self.reader, &own).map_err(fault)?.into_iter().map(|target| (target.model.to_string(), target.id.to_string())).collect())
    }
}
