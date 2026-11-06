use serde_json::Value;

use crate::schema::FieldSpec;
use crate::owners::write::upload::UploadOperations;
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

/// The media of a message goes through `send_file`: the row is written like a `create`, the final mutation carries the signed id of the file.
pub(super) fn add_message_file(models: &mut Value) {
    add_file_action(models, "Message", "send_file", "SendMessageFile", "sendMessageFile");
}

/// A moment is published with its file through `create_file`.
pub(super) fn add_moment_file(models: &mut Value) {
    add_file_action(models, "Moment", "create_file", "MomentCreateFile", "momentCreateFile");
}

fn add_file_action(models: &mut Value, model: &str, action: &str, mutation: &str, root: &str) {
    let steps = serde_json::to_value(UploadOperations::test_steps()).unwrap();
    let spec = models.as_array_mut().unwrap().iter_mut().find(|candidate| candidate["key"] == model).unwrap();
    let mut declared = crate::schema_fixtures::upload_declared(action);
    declared.mutation = Some(mutation.into());
    declared.root = Some(root.into());
    declared.has_optimistic = true;
    declared.has_temp_row_retention = true;
    spec["actions"].as_array_mut().unwrap().push(serde_json::to_value(declared).unwrap());
    spec["upload_steps"] = steps;
}
