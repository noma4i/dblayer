use crate::parse_models;

const PLAIN: &str = "model Chat do\n  field :rank, type: :enum, values: [:a, :b], as: Level\nend\n";

fn conflicts(second: &str) -> Vec<(usize, String)> {
    let source = format!("{PLAIN}model Mail do\n{second}end\n");
    parse_models(&source).err().unwrap_or_default().into_iter().map(|error| (error.line, error.message)).collect()
}

#[test]
fn one_enum_name_with_different_values_is_refused_wherever_either_declaration_sits() {
    let cases = [
        ("a union variant", "  field :card, type: MailCard do\n    on CardText do\n      field :tone, type: :enum, values: [:x], as: Level\n    end\n  end\n"),
        ("a shared field of an interface", "  field :card, type: MailCard do\n    on CardText do\n      field :id, type: :id\n    end\n    shared do\n      field :tone, type: :enum, values: [:x], as: Level, null: true\n    end\n  end\n"),
        ("an edge", "  scope :feed do\n    fetch MailFeed\n    paginate 20\n    edge :rank, type: :enum, values: [:x], as: Level, null: true\n  end\n"),
        ("a nested object", "  field :meta, type: MailMeta, null: true do\n    field :tags, type: [:enum], values: [:x], as: Level\n  end\n"),
    ];
    let accepted: Vec<&str> = cases.iter().filter(|(_, second)| !conflicts(second).iter().any(|(_, message)| message.contains("enum `Level` is declared with different values"))).map(|(name, _)| *name).collect();
    assert!(accepted.is_empty(), "accepted: {accepted:?}");
}

#[test]
fn a_conflict_is_reported_at_both_declarations() {
    let found = conflicts("  field :card, type: MailCard do\n    on CardText do\n      field :tone, type: :enum, values: [:x], as: Level\n    end\n  end\n");
    let lines: Vec<usize> = found.iter().map(|(line, _)| *line).collect();
    assert_eq!(lines, [2, 7], "{found:?}");
}
