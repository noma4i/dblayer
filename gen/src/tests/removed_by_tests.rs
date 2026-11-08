use crate::parse_models;

const SOURCES: &str = "model Message do\n  attribute :chatId, :id\n  attribute :authorId, :id\nend\nmodel Action do\n  attribute :chatId, :id\n  attribute :userId, :id\n";

/// M40: `removed_by` reaches the spec and names a model and fields of equal kind.
#[test]
fn a_model_declares_the_model_whose_rows_remove_its_rows() {
    let source = |line: &str| format!("{SOURCES}  {line}\nend\n");
    let models = parse_models(&source("removed_by Message, match: { chatId: :chatId, userId: :authorId }")).unwrap();
    let spec = serde_json::to_value(&models[1]).unwrap();
    assert_eq!(spec["removed_by"], serde_json::json!([{"model": "Message", "matches": {"chatId": "chatId", "userId": "authorId"}}]));

    let cases = [
        ("unknown model", "removed_by Nothing, match: { chatId: :chatId }", "unknown model `Nothing`"),
        ("unknown own field", "removed_by Message, match: { room: :chatId }", "which `Action` does not declare"),
        ("unknown field of the other model", "removed_by Message, match: { chatId: :room }", "which `Message` does not declare"),
        ("no match", "removed_by Message", "needs `match:`"),
        ("empty match", "removed_by Message, match: {}", "`match` is `{ field: :model_field }`"),
    ];
    for (case, line, message) in cases {
        let errors = parse_models(&source(line)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(message)), "{case}: {errors:?}");
    }
}
