use super::*;

#[test]
fn removes_on_requires_a_node_and_known_removal_kinds() {
    let source = "model Chat do\n field :title, type: :string\n node ChatNode\n scope :current do\n fetch CurrentChat\n end\n changed_by :chat, via: :node, removes_on: [:deleted, :access_revoked]\nend";
    let models = crate::parse_models(source).unwrap();
    assert_eq!(serde_json::to_value(&models[0]).unwrap()["changes"][0]["removes_on"], serde_json::json!(["DELETED", "ACCESS_REVOKED"]));
    for (input, expected) in [
        (source.replace("via: :node", "via: :current"), "via: :node"),
        (source.replace(":deleted, :access_revoked", ":updated"), "unknown removal kind"),
    ] {
        let errors = crate::parse_models(&input).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(expected) && error.line == 7 && error.column > 0), "{errors:?}");
    }
}

#[test]
fn changes_declarations_resolve_remote_reads_and_reject_ambiguous_targets() {
    let model = |name: &str, declaration: &str| format!("model {name} do\n schema do\n string :title\n end\n remote :node do\n query Read\n end\n {declaration}\nend\n");
    let valid = model("Chat", "changed_by :chat, via: :node");
    let models = check_source(&valid).unwrap();
    let serialized = serde_json::to_value(&models[0]).unwrap();
    assert_eq!(serialized["changes"][0]["area"], "CHAT");
    assert_eq!(serialized["changes"][0]["relation"], "node");
    for (declaration, message) in [
        ("changed_by :unknown, via: :node", "unknown change area"),
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
    let source = include_str!("../../../fixtures/v3-cable/changes.dbl");
    let models = crate::parse_models(source).unwrap();
    assert_eq!(models.iter().flat_map(|model| &model.changes).filter(|change| change.area == "REACTION").count(), 2);
    let invalid = source.replace("changed_by :counters", "changed_by :reaction");
    let errors = crate::parse_models(&invalid).unwrap_err();
    assert!(errors.iter().any(|error| error.message.contains("duplicate change area")), "{errors:?}");
}

#[test]
fn local_updates_require_memory_storage_and_unique_subscription_names() {
    let source = "model Notice, storage: :memory, expire_after: 1.minute do\n field :body, type: :string\n scope :recent do\n updates ViewerNotice\n end\nend";
    assert!(crate::parse_models(source).is_ok(), "{:?}", crate::parse_models(source));
    for (input, expected) in [
        (source.replace(", storage: :memory, expire_after: 1.minute", ""), "memory"),
        (source.replace("updates ViewerNotice", "updates ViewerNotice\n updates ViewerNotice"), "duplicate subscription"),
    ] {
        let errors = crate::parse_models(&input).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(expected)), "{errors:?}");
    }
}
