use super::*;

#[test]
fn subscription_actions_validate_effects_and_required_data() {
    let source = |apply: &str| format!("model Record do\n field :title, type: :string\n node RecordNode\n listen RecordSignal, root: \"recordSignal\", id: \"recordId\", apply: {{{apply}}}\nend");
    for apply in ["effect: :ephemeral, fields: {typing: :active}, matches: {kind: \"ACTIVE\"}", "effect: :reread, via: :node", "effect: :ignore"] {
        let models = crate::parse_models(&source(apply)).unwrap();
        assert!(serde_json::to_value(&models[0]).unwrap()["listens"][0]["apply"].is_object());
    }
    for (apply, expected) in [("effect: :unknown", "unknown variant"), ("effect: :ephemeral, fields: {}", "fields"), ("effect: :reread", "via")] {
        let errors = crate::parse_models(&source(apply)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(expected)), "{errors:?}");
    }
}

#[test]
fn application_declares_change_subscription_and_payload_paths() {
    let source = "model Record do\n node RecordNode\n changes RecordEvents, root: \"recordEvents.entries\", area: :topic, kind: :action, id: :recordId\n changed_by :record, via: :node, removes_on: [:erased]\nend";
    let models = crate::parse_models(source).unwrap();
    let value = serde_json::to_value(&models[0]).unwrap();
    assert_eq!(value["change_feed"], serde_json::json!({"subscription":"RecordEvents", "root":"recordEvents.entries", "area":"topic", "kind":"action", "id":"recordId"}));
}

#[test]
fn removes_on_requires_a_node() {
    let source = "model Chat do\n changes RecordEvents, root: \"recordEvents.entries\", area: :topic, kind: :action, id: :recordId\n field :title, type: :string\n node ChatNode\n scope :current do\n fetch CurrentChat\n end\n changed_by :chat, via: :node, removes_on: [:deleted, :access_revoked]\nend";
    let models = crate::parse_models(source).unwrap();
    assert_eq!(serde_json::to_value(&models[0]).unwrap()["changes"][0]["removes_on"], serde_json::json!(["DELETED", "ACCESS_REVOKED"]));
    for (input, expected) in [
        (source.replace("via: :node", "via: :current"), "via: :node"),
    ] {
        let errors = crate::parse_models(&input).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(expected) && error.line == 8 && error.column > 0), "{errors:?}");
    }
}

#[test]
fn changes_declarations_resolve_remote_reads_and_reject_ambiguous_targets() {
    let model = |name: &str, declaration: &str| format!("model {name} do\n schema do\n string :title\n end\n remote :node do\n query Read\n end\n changes RecordEvents, root: \"recordEvents.entries\", area: :topic, kind: :action, id: :recordId\n {declaration}\nend\n");
    let valid = model("Chat", "changed_by :chat, via: :node");
    let models = check_source(&valid).unwrap();
    let serialized = serde_json::to_value(&models[0]).unwrap();
    assert_eq!(serialized["changes"][0]["area"], "CHAT");
    assert_eq!(serialized["changes"][0]["relation"], "node");
    for (declaration, message) in [
        ("changed_by :chat, via: :absent", "unknown relation"),
        ("changed_by :chat, via: :node, id: :absent", "parameter"),
    ] {
        let errors = check_source(&model("Chat", declaration)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(message)), "{errors:?}");
        assert!(errors.iter().all(|error| error.line > 0 && error.column > 0));
    }
}

#[test]
fn public_changes_declarations_accept_shared_node_areas_and_reject_shared_lists() {
    let source = include_str!("../../../fixtures/change-events/changes.dbl");
    let models = crate::parse_models(source).unwrap();
    assert_eq!(models.iter().flat_map(|model| &model.changes).filter(|change| change.area == "REACTION").count(), 2);
    let invalid = source.replace("changed_by :counters", "changed_by :reaction");
    let errors = crate::parse_models(&invalid).unwrap_err();
    assert!(errors.iter().any(|error| error.message.contains("duplicate change area")), "{errors:?}");
}

#[test]
fn local_updates_require_memory_storage_and_unique_subscription_names() {
    let source = "model Notice, storage: :memory, expire_after: 1.minute do\n field :body, type: :string\n scope :recent do\n updates BulletinAdded\n end\nend";
    assert!(crate::parse_models(source).is_ok(), "{:?}", crate::parse_models(source));
    for (input, expected) in [
        (source.replace(", storage: :memory, expire_after: 1.minute", ""), "memory"),
        (source.replace("updates BulletinAdded", "updates BulletinAdded\n updates BulletinAdded"), "duplicate subscription"),
    ] {
        let errors = crate::parse_models(&input).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(expected)), "{errors:?}");
    }
}
