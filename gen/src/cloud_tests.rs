use crate::parse_models;

const LOCAL: &str = "model Row, storage: :local do\n  attribute :title, :string\n  attribute :pinned, :bool, default: false\n  attribute :note, :string, null: true\n  ephemeral do\n    attribute :typing, :bool, default: false\n  end\n";
const DISK: &str = "model Row do\n  attribute :title, :string\n  attribute :tone, :string, optional: true, local: true\n  attribute :mark, :string, optional: true, local: true\n";

#[test]
fn invalid_cloud_declarations_are_rejected_with_a_reason() {
    let cases = [
        (DISK, "cloud", "needs attribute names"),
        (DISK, "cloud :title", "server attribute `title`"),
        (DISK, "cloud :tone, :title", "server attribute `title`"),
        ("model Row, storage: :memory do\n  attribute :title, :string\n", "cloud", "not allowed with `storage: :memory`"),
        ("model Row, storage: :memory do\n  attribute :title, :string\n", "cloud :title", "not allowed with `storage: :memory`"),
        (LOCAL, "cloud :typing", "ephemeral attribute `typing`"),
        (LOCAL, "cloud :userId", "unknown attribute `userId`"),
        (LOCAL, "cloud :page", "a parameter is not an attribute"),
        (LOCAL, "cloud :title, :pinned\n  cloud :note", "`cloud` is declared twice"),
        (LOCAL, "cloud :title, :title", "lists `title` twice"),
        (LOCAL, "cloud :pinned", "attribute `title` is required and has no default"),
        ("model Row, storage: :local do\n  ephemeral do\n    attribute :typing, :bool, default: false\n  end\n", "cloud", "selects no attribute"),
        (LOCAL, "cloud storage: :local", "unknown key `storage` for `cloud`"),
    ];
    for (head, line, message) in cases {
        let errors = parse_models(&format!("{head}  {line}\nend\n")).expect_err(line);
        assert!(errors.iter().any(|error| error.message.contains(message)), "{line}: {errors:?}");
    }
}

#[test]
fn cloud_expands_to_the_selected_attribute_names() {
    let cases = [
        (LOCAL, "cloud", vec!["title", "pinned", "note"]),
        (LOCAL, "cloud :note, :title", vec!["note", "title"]),
        ("model Row, storage: :local do\n  attribute :title, :string\n  attribute :pinned, :bool, default: false\n", "cloud :title", vec!["title"]),
        (DISK, "cloud :mark, :tone", vec!["mark", "tone"]),
    ];
    for (head, line, expected) in cases {
        let models = parse_models(&format!("{head}  {line}\nend\n")).unwrap();
        assert_eq!(models[0].cloud, expected, "{line}");
    }
}

#[test]
fn spec_json_has_the_cloud_key_only_when_the_word_is_declared() {
    let with = parse_models(&format!("{LOCAL}  cloud :title, :pinned, :note\nend\n")).unwrap();
    let without = parse_models(&format!("{LOCAL}end\n")).unwrap();
    assert_eq!(serde_json::to_value(&with[0]).unwrap()["cloud"], serde_json::json!(["title", "pinned", "note"]));
    assert!(serde_json::to_value(&without[0]).unwrap().get("cloud").is_none());
}
