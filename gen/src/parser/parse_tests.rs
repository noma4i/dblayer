use super::*;
use crate::lexer::lex;

fn parse_source(source: &str) -> Result<Vec<ModelNode>, ParseError> {
    parse(&lex(source).unwrap(), 0).map(|parsed| parsed.models)
}

fn symbol(text: &str) -> Value {
    Value::Symbol(text.to_string())
}

#[test]
fn empty_and_comment_only_files_have_no_models() {
    assert_eq!(parse_source("").unwrap(), vec![]);
    assert_eq!(parse_source("# nothing\n\n").unwrap(), vec![]);
}

#[test]
fn model_block_holds_calls() {
    let models = parse_source("model User do\n  schema do\n    string :name\n  end\nend\n").unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!((models[0].name.as_str(), models[0].line), ("User", 1));
    let schema = &models[0].body[0];
    assert_eq!((schema.word.as_str(), schema.line), ("schema", 2));
    let field = &schema.block.as_ref().unwrap()[0];
    assert_eq!((field.word.as_str(), field.line), ("string", 3));
    assert_eq!(field.args[0].item, symbol("name"));
}

#[test]
fn two_models_in_one_file() {
    let models = parse_source("model A do\nend\nmodel B do\nend").unwrap();
    assert_eq!(models.iter().map(|model| model.name.as_str()).collect::<Vec<_>>(), vec!["A", "B"]);
    assert_eq!(models[1].line, 3);
}

#[test]
fn options_arrays_hashes_and_durations() {
    let models = parse_source(
        "model M do\n  scope :by_user, params: [:userId], where: { userId: :userId }, order: { createdAt: :desc }\n  fresh_for 5.minutes, empty: 30.seconds\nend",
    )
    .unwrap();
    let scope = &models[0].body[0];
    assert_eq!(scope.args[0].item, symbol("by_user"));
    assert_eq!(scope.options.iter().map(|pair| pair.key.as_str()).collect::<Vec<_>>(), vec!["params", "where", "order"]);
    assert!(matches!(&scope.options[0].value.item, Value::Array(items) if items.len() == 1));
    assert!(matches!(&scope.options[1].value.item, Value::Hash(pairs) if pairs[0].key == "userId"));
    let fresh = &models[0].body[1];
    assert_eq!(fresh.args[0].item, Value::Duration { amount: 5, unit: "minutes".into() });
    assert_eq!(fresh.options[0].value.item, Value::Duration { amount: 30, unit: "seconds".into() });
}

#[test]
fn continuation_after_comma_and_multiline_collections() {
    let models = parse_source("model M do\n  scope :a,\n    where: {\n      x: :x,\n      y: :y\n    }\nend").unwrap();
    let scope = &models[0].body[0];
    assert!(matches!(&scope.options[0].value.item, Value::Hash(pairs) if pairs.len() == 2));
}

#[test]
fn bare_and_empty_literals() {
    let models = parse_source("model M do\n  object :media, default: {}\n  list :tags, of: :string, default: []\n  query MomentsFeed\n  list\nend").unwrap();
    assert_eq!(models[0].body[0].options[0].value.item, Value::Hash(vec![]));
    assert_eq!(models[0].body[1].options[1].value.item, Value::Array(vec![]));
    assert_eq!(models[0].body[2].args[0].item, Value::Const("MomentsFeed".into()));
    assert_eq!(models[0].body[3].args, vec![]);
}

#[test]
fn malformed_sources_fail_with_a_line_and_a_message() {
    let cases: [(&str, &str, Option<usize>, Option<&str>); 8] = [
        ("top level word other than model", "\nstring :name\n", Some(2), Some("expected `model`, `operation` or `scalar`, found `string`")),
        ("model without name", "model do\nend", Some(1), Some("model needs a constant name, for example `model User do`")),
        ("model without block", "model User\n", Some(1), Some("model User needs a `do ... end` block")),
        ("unclosed block", "model User do\n  schema do\n    string :a\nend\n", Some(1), Some("`model` block is not closed with `end`")),
        ("stray end", "model A do\nend\nend", Some(3), Some("expected `model`, `operation` or `scalar`, found `end`")),
        ("trailing comma", "model A do\n  index :a,", None, Some("unexpected end of file")),
        ("unexpected token", "model A do\n  index :a :b\nend", Some(2), Some("unexpected `:b`")),
        ("unclosed array", "model A do\n  scope :a, params: [:x\nend", Some(3), None),
    ];
    for (case, source, line, message) in cases {
        let error = parse_source(source).unwrap_err();
        if let Some(line) = line {
            assert_eq!(error.line, line, "{case}: line");
        }
        if let Some(message) = message {
            assert_eq!(error.message, message, "{case}: message");
        }
    }
}

#[test]
fn model_line_takes_options_before_the_block() {
    let models = parse_source("model Presence, storage: :memory, expire_after: 30.seconds do\n  schema do\n    string :room\n  end\nend").unwrap();
    let options: Vec<(&str, Value)> = models[0].options.iter().map(|pair| (pair.key.as_str(), pair.value.item.clone())).collect();
    assert_eq!(options, vec![("storage", symbol("memory")), ("expire_after", Value::Duration { amount: 30, unit: "seconds".into() })]);
    assert_eq!(models[0].name, "Presence");
}

#[test]
fn nested_block_inside_schema_keeps_its_fields() {
    let models = parse_source("model User do\n  schema do\n    string :name\n    ephemeral do\n      bool :typing, default: false\n    end\n  end\nend").unwrap();
    let schema = models[0].body[0].block.as_ref().unwrap();
    let ephemeral = schema[1].block.as_ref().unwrap();
    assert_eq!((schema[1].word.as_str(), ephemeral[0].word.as_str(), ephemeral[0].line), ("ephemeral", "bool", 5));
}
