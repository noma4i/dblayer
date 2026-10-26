use super::check_source;
use crate::parse_models;

const NODE: &str = "  scope :node, node: :one do\n    parameter :id, :id\n    query \"RecordNode\"\n    variables id: :id\n  end\n";

fn subscription(apply: &str) -> String {
    format!("model Record do\n  attribute :title, :string\n  attribute :active, :bool, default: false, null: true\n{NODE}  subscribe \"RecordSignal\", from: \"recordSignal\", id: \"recordId\", apply: {{ {apply} }}\nend")
}

#[test]
fn subscribe_applies_one_closed_effect_with_the_data_that_effect_needs() {
    for apply in [
        "effect: :ephemeral, fields: { active: \"typing.active\" }, matches: { kind: \"ACTIVE\" }",
        "effect: :reread, via: :node",
        "effect: :ignore",
        "effect: :upsert",
        "effect: :upsert, delete_if: \"gone\"",
        "effect: :delete",
    ] {
        let models = parse_models(&subscription(apply)).unwrap_or_else(|errors| panic!("{apply}: {errors:?}"));
        let value = serde_json::to_value(&models[0]).unwrap();
        let has_apply = value["listens"][0]["apply"].is_object();
        assert_eq!(has_apply, !apply.starts_with("effect: :upsert") && apply != "effect: :delete", "{apply}");
    }
    for (apply, expected) in [
        ("effect: :unknown", "`effect` must be"),
        ("effect: :ephemeral", "ephemeral apply requires nonempty fields and paths"),
        ("effect: :ephemeral, fields: { active: \"a..b\" }", "has an empty segment"),
        ("effect: :reread", "reread apply requires via"),
        ("effect: :ignore, via: :node", "`via` applies only to effect `:reread`"),
        ("effect: :ignore, fields: { a: \"b\" }", "`fields` applies only to effect `:ephemeral`"),
        ("effect: :ignore, values: { active: true }", "`values` applies only to effect `:upsert`"),
        ("effect: :delete, delete_if: \"gone\"", "`delete_if` applies only to effect `:upsert`"),
        ("effect: :upsert, matches: { kind: \"A\" }", "`matches` applies only to effect `:ephemeral`, `:reread` and `:ignore`"),
        ("effect: :ignore, matches: { kind: 1 }", "a `matches` value must be a string"),
        ("fields: { a: \"b\" }", "`apply` needs `effect:`"),
        ("effect: :ignore, junk: 1", "unknown key `junk` for `apply`"),
    ] {
        let errors = parse_models(&subscription(apply)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(expected)), "{apply}: {errors:?}");
    }
}

#[test]
fn subscribe_reads_its_name_paths_and_debounce_as_strings() {
    let model = |line: &str| format!("model Record do\n  attribute :title, :string\n  {line}\nend");
    parse_models(&model("subscribe \"Signal\", from: \"a.b\", id: \"id\", debounce: 100.milliseconds")).unwrap();
    for (line, expected) in [
        ("subscribe Signal", "the subscription name must be a string"),
        ("subscribe", "`subscribe` needs a subscription name"),
        ("subscribe \"Signal\", root: \"a.b\"", "unknown key `root` for `subscribe`"),
        ("subscribe \"Signal\", from: :a", "`from` must be a string"),
        ("subscribe \"Signal\", id: \"a..b\"", "has an empty segment"),
        ("subscribe \"Signal\", debounce: 100", "`debounce` must be a duration"),
        ("listen \"Signal\"", "unknown model declaration `listen`"),
        ("subscribe \"Signal\"\n  subscribe \"Signal\"", "duplicate subscription `Signal`"),
    ] {
        let errors = parse_models(&model(line)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(expected)), "{line}: {errors:?}");
    }
}

#[test]
fn apply_values_need_an_ephemeral_attribute_that_resets() {
    let source = "model Record do\n  attribute :title, :string\n  ephemeral do\n    attribute :typing, :bool, default: false, reset_after: 5.seconds\n  end\n  scope :one do\n    query \"One\"\n    subscribe \"Typing\", apply: { effect: :upsert, values: { typing: true } }\n  end\nend";
    let models = parse_models(source).unwrap();
    assert_eq!(serde_json::to_value(&models[0]).unwrap()["relations"][0]["updates"][0]["set"]["typing"], serde_json::json!(true));
    let errors = parse_models(&source.replace(", reset_after: 5.seconds", "")).unwrap_err();
    assert!(errors.iter().any(|error| error.message.contains("reset")), "{errors:?}");
}

#[test]
fn changes_declares_the_subscription_and_payload_paths() {
    let source = format!("model Record do\n  attribute :title, :string\n{NODE}  changes \"RecordEvents\", from: \"recordEvents.entries\", area: \"topic\", kind: \"action\", id: \"recordId\"\n  changed_by \"RECORD\", via: :node, removes_on: [\"ERASED\"]\nend");
    let models = parse_models(&source).unwrap();
    let value = serde_json::to_value(&models[0]).unwrap();
    assert_eq!(value["change_feed"], serde_json::json!({"subscription":"RecordEvents", "root":"recordEvents.entries", "area":"topic", "kind":"action", "id":"recordId"}));
    for (replaced, with, expected) in [
        ("from: \"recordEvents.entries\", ", "", "`changes` needs `from:`"),
        ("area: \"topic\", ", "", "`changes` needs `area:`"),
        ("kind: \"action\", ", "", "`changes` needs `kind:`"),
        (", id: \"recordId\"", "", "`changes` needs `id:`"),
        ("area: \"topic\"", "area: :topic", "`area` must be a string"),
        ("from: \"recordEvents.entries\"", "root: \"recordEvents.entries\"", "unknown key `root` for `changes`"),
        ("\"RecordEvents\"", "RecordEvents", "the subscription name must be a string"),
        ("id: \"recordId\"", "id: \"a..b\"", "has an empty segment"),
    ] {
        let errors = parse_models(&source.replace(replaced, with)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(expected)), "{replaced}: {errors:?}");
    }
    let twice = format!("{source}\nmodel Other do\n  changes \"OtherEvents\", from: \"a.b\", area: \"topic\", kind: \"action\", id: \"id\"\nend");
    let errors = parse_models(&twice).unwrap_err();
    assert!(errors.iter().any(|error| error.message.contains("duplicate changes subscription")), "{errors:?}");
}

#[test]
fn removes_on_needs_a_via_scope_declared_as_a_node_whatever_its_name() {
    let source = "model Record do\n  changes \"RecordEvents\", from: \"recordEvents.entries\", area: \"topic\", kind: \"action\", id: \"recordId\"\n  attribute :title, :string\n  scope :by_id, node: :one do\n    parameter :id, :id\n    query \"RecordNode\"\n    variables id: :id\n  end\n  scope :current do\n    query \"CurrentRecord\"\n  end\n  changed_by \"RECORD\", via: :by_id, removes_on: [\"DELETED\", \"ACCESS_REVOKED\"]\nend";
    let models = parse_models(source).unwrap();
    assert_eq!(serde_json::to_value(&models[0]).unwrap()["changes"][0]["removes_on"], serde_json::json!(["DELETED", "ACCESS_REVOKED"]));
    let errors = parse_models(&source.replace("via: :by_id", "via: :current")).unwrap_err();
    assert!(errors.iter().any(|error| error.message.contains("removes_on requires a via scope declared with `node: :one`") && error.line == 12 && error.column > 0), "{errors:?}");
}

#[test]
fn changed_by_resolves_remote_reads_and_rejects_ambiguous_targets() {
    let model = |declaration: &str| format!("model Record do\n  attribute :title, :string\n  scope :read do\n    query \"Read\"\n  end\n  changes \"RecordEvents\", from: \"recordEvents.entries\", area: \"topic\", kind: \"action\", id: \"recordId\"\n  {declaration}\nend\n");
    let models = check_source(&model("changed_by \"RECORD\", via: :read")).unwrap();
    let serialized = serde_json::to_value(&models[0]).unwrap();
    assert_eq!(serialized["changes"][0]["area"], "RECORD");
    assert_eq!(serialized["changes"][0]["relation"], "read");
    for (declaration, message) in [
        ("changed_by \"RECORD\", via: :absent", "unknown relation"),
        ("changed_by \"RECORD\", via: :read, parameter: :absent", "parameter"),
        ("changed_by \"RECORD\"", "`changed_by` needs `via:`"),
        ("changed_by :record, via: :read", "the area must be a string"),
        ("changed_by \"RECORD\", via: \"read\"", "`via` must be a symbol"),
        ("changed_by \"RECORD\", via: :read, id: :x", "unknown key `id` for `changed_by`"),
        ("changed_by \"RECORD\", via: :read, removes_on: [:deleted]", "`removes_on` must be a list of strings"),
        ("changed_by \"RECORD\", via: :read\n  changed_by \"RECORD\", via: :read", "duplicate change area `RECORD`"),
    ] {
        let errors = check_source(&model(declaration)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(message)), "{declaration}: {errors:?}");
        assert!(errors.iter().all(|error| error.line > 0 && error.column > 0));
    }
}

#[test]
fn changed_by_needs_a_changes_subscription() {
    let errors = check_source("model Record do\n  attribute :title, :string\n  scope :read do\n    query \"Read\"\n  end\n  changed_by \"RECORD\", via: :read\nend\n").unwrap_err();
    assert!(errors.iter().any(|error| error.message.contains("changed_by requires a changes subscription")), "{errors:?}");
}

#[test]
fn one_area_on_several_models_is_accepted_and_twice_in_one_model_is_refused() {
    let scopes = "  scope :by_id, node: :one do\n    parameter :id, :id\n    query \"ById\"\n    variables id: :id\n  end\n  scope :read do\n    query \"Read\"\n  end\n";
    let feed = "  changes \"RecordEvents\", from: \"recordEvents.entries\", area: \"topic\", kind: \"action\", id: \"recordId\"\n";
    let sources = |first: &str, second: &str| format!("model First do\n  attribute :title, :string\n{scopes}{feed}  {first}\nend\n\nmodel Second do\n  attribute :title, :string\n{scopes}  {second}\nend\n");
    for (declaration, area) in [("changed_by \"WALLET\", via: :observed", "WALLET"), ("changed_by \"REACTION\", via: :by_id", "REACTION"), ("changed_by \"MOOD\", via: :read", "MOOD")] {
        let models = check_source(&sources(declaration, declaration)).unwrap();
        assert!(models.iter().all(|model| model.changes.len() == 1 && model.changes[0].area == area), "{declaration}");
        let twice = format!("{declaration}\n  {declaration}");
        for source in [sources(&twice, declaration), sources(declaration, &twice)] {
            let errors = check_source(&source).unwrap_err();
            assert!(errors.iter().any(|error| error.message.contains(&format!("duplicate change area `{area}`"))), "{declaration}: {errors:?}");
        }
    }
}

#[test]
fn local_updates_require_memory_storage_and_unique_subscription_names() {
    let source = "model Notice, storage: :memory, expires_in: 1.minute do\n  attribute :body, :string\n  scope :recent do\n    subscribe \"BulletinAdded\"\n  end\nend";
    assert!(parse_models(source).is_ok(), "{:?}", parse_models(source));
    for (input, expected) in [
        (source.replace(", storage: :memory, expires_in: 1.minute", ""), "memory"),
        (source.replace("subscribe \"BulletinAdded\"", "subscribe \"BulletinAdded\"\n    subscribe \"BulletinAdded\""), "duplicate subscription"),
    ] {
        let errors = parse_models(&input).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(expected)), "{errors:?}");
    }
}

#[test]
fn observed_rereads_need_a_remote_read_and_take_no_node_arguments() {
    let model = |declaration: &str| format!("model Record do\n  attribute :title, :string\n  scope :read do\n    query \"Read\"\n  end\n  changes \"RecordEvents\", from: \"recordEvents.entries\", area: \"topic\", kind: \"action\", id: \"recordId\"\n  {declaration}\nend\n");
    let models = check_source(&model("changed_by \"WALLET\", via: :observed")).unwrap();
    let serialized = serde_json::to_value(&models[0]).unwrap();
    assert_eq!(serialized["changes"][0]["observed"], true);
    for (declaration, message) in [
        ("changed_by \"WALLET\", via: :observed, removes_on: [\"GONE\"]", "removes_on cannot be combined with `via: :observed`"),
        ("changed_by \"WALLET\", via: :observed, parameter: :kind", "`via: :observed` takes no parameter"),
        ("changed_by \"WALLET\", via: :observed\n  changed_by \"WALLET\", via: :observed", "duplicate change area `WALLET`"),
    ] {
        let errors = check_source(&model(declaration)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(message)), "{declaration}: {errors:?}");
    }
    let local = "model Record do\n  attribute :title, :string\n  changes \"RecordEvents\", from: \"recordEvents.entries\", area: \"topic\", kind: \"action\", id: \"recordId\"\n  changed_by \"WALLET\", via: :observed\nend\n";
    let errors = check_source(local).unwrap_err();
    assert!(errors.iter().any(|error| error.message.contains("requires a model with a remote read")), "{errors:?}");
}

#[test]
fn only_models_with_changed_by_get_an_events_stream() {
    let source = "model Record do\n  attribute :title, :string\n  scope :read do\n    query \"Read\"\n  end\n  changes \"RecordEvents\", from: \"recordEvents.entries\", area: \"topic\", kind: \"action\", id: \"recordId\"\n  changed_by \"WALLET\", via: :observed\nend\n\nmodel Plain do\n  attribute :title, :string\nend\n";
    let models = parse_models(source).unwrap();
    let swift = crate::emit_swift(&models, &[], &Default::default());
    let kotlin = crate::emit_kotlin(&models, &[], &Default::default());
    assert_eq!(swift.matches("static var events: LiveStream<ModelEventItem<Self>>").count(), 1);
    assert_eq!(kotlin.matches("val events: kotlinx.coroutines.flow.Flow<ModelEventItem<Record>>").count(), 1);
}

#[test]
fn node_and_observed_forms_share_an_area_and_equal_forms_stay_refused() {
    let source = |lines: &str| format!("model Record do\n  attribute :title, :string\n  scope :by_id, node: :one do\n    parameter :id, :id\n    query \"ById\"\n    variables id: :id\n  end\n  scope :read do\n    query \"Read\"\n  end\n  changes \"RecordEvents\", from: \"recordEvents.entries\", area: \"topic\", kind: \"action\", id: \"recordId\"\n  {lines}\nend\n");
    let models = check_source(&source("changed_by \"CHAT\", via: :by_id, removes_on: [\"DELETED\"]\n  changed_by \"CHAT\", via: :observed")).unwrap();
    let forms: Vec<_> = models[0].changes.iter().map(|change| (change.area.as_str(), change.observed)).collect();
    assert_eq!(forms, [("CHAT", false), ("CHAT", true)]);
    for lines in ["changed_by \"CHAT\", via: :observed\n  changed_by \"CHAT\", via: :observed", "changed_by \"CHAT\", via: :by_id\n  changed_by \"CHAT\", via: :read"] {
        let errors = check_source(&source(lines)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains("duplicate change area `CHAT`")), "{lines}: {errors:?}");
    }
}
