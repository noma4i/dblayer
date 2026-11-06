use serde_json::Value;

use crate::schema::FieldSpec;
use crate::upload::UploadOperations;
use crate::value::ScalarKind;

/// The photo of a chat and the steps of the project.
pub(super) fn add_uploads(models: &mut Value) {
    let steps = serde_json::to_value(UploadOperations::test_steps()).unwrap();
    let models = models.as_array_mut().unwrap();
    for (model, action, mutation) in [("Chat", "update_photo", "ChatUpdatePhoto")] {
        let Some(spec) = models.iter_mut().find(|candidate| candidate["key"] == model) else { continue };
        let mut declared = crate::schema_fixtures::upload_declared(action);
        declared.mutation = Some(mutation.into());
        declared.on_failure = crate::schema::OnFailure::Keep;
        spec["actions"].as_array_mut().unwrap().push(serde_json::to_value(declared).unwrap());
        spec["upload_steps"] = steps.clone();
    }
    if let Some(chat) = models.iter_mut().find(|candidate| candidate["key"] == "Chat") {
        chat["fields"].as_array_mut().unwrap().push(serde_json::to_value(FieldSpec::scalar("logoUrl", ScalarKind::Str).nullable().optional()).unwrap());
    }
}
