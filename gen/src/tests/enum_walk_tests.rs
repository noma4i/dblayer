use crate::parse_models;

const PLAIN: &str = "model Chat do\n  attribute :rank, :enum, values: [\"a\", \"b\"], as: Level\nend\n";

fn conflicts(second: &str) -> Vec<(usize, String)> {
    let source = format!("{PLAIN}model Mail do\n{second}end\n");
    parse_models(&source).err().unwrap_or_default().into_iter().map(|error| (error.line, error.message)).collect()
}

#[test]
fn one_enum_name_with_different_values_is_refused_wherever_either_declaration_sits() {
    let cases = [
        ("a union variant", "  attribute :card, MailCard do\n    on CardText do\n      attribute :tone, :enum, values: [\"x\"], as: Level\n    end\n  end\n"),
        ("a shared field of an interface", "  attribute :card, MailCard do\n    on CardText do\n      attribute :id, :id\n    end\n    shared do\n      attribute :tone, :enum, values: [\"x\"], as: Level, null: true\n    end\n  end\n"),
        ("an edge", "  scope :feed do\n    query \"MailFeed\"\n    paginate size: 20, direction: :forward\n    edge do\n      attribute :rank, :enum, values: [\"x\"], as: Level, null: true\n    end\n  end\n"),
        ("a nested object", "  attribute :meta, MailMeta, null: true do\n    attribute :tags, [:enum], values: [\"x\"], as: Level\n  end\n"),
    ];
    let accepted: Vec<&str> = cases.iter().filter(|(_, second)| !conflicts(second).iter().any(|(_, message)| message.contains("enum `Level` is declared with different values"))).map(|(name, _)| *name).collect();
    assert!(accepted.is_empty(), "accepted: {accepted:?}");
}

#[test]
fn a_conflict_is_reported_at_both_declarations() {
    let found = conflicts("  attribute :card, MailCard do\n    on CardText do\n      attribute :tone, :enum, values: [\"x\"], as: Level\n    end\n  end\n");
    let lines: Vec<usize> = found.iter().map(|(line, _)| *line).collect();
    assert_eq!(lines, [2, 7], "{found:?}");
}
