use super::*;
use crate::lexer::lex;

fn parse_source(source: &str) -> Result<Vec<ModelNode>, ParseError> {
    parse(&lex(source).unwrap(), 0)
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
fn top_level_word_other_than_model_fails() {
    let error = parse_source("\nstring :name\n").unwrap_err();
    assert_eq!((error.line, error.message.as_str()), (2, "expected `model`, found `string`"));
}

#[test]
fn model_without_name_or_block_fails() {
    let error = parse_source("model do\nend").unwrap_err();
    assert_eq!((error.line, error.message.as_str()), (1, "model needs a constant name, for example `model User do`"));
    let error = parse_source("model User\n").unwrap_err();
    assert_eq!((error.line, error.message.as_str()), (1, "model User needs a `do ... end` block"));
}

#[test]
fn unclosed_block_points_at_opener() {
    let error = parse_source("model User do\n  schema do\n    string :a\nend\n").unwrap_err();
    assert_eq!((error.line, error.message.as_str()), (1, "`model` block is not closed with `end`"));
}

#[test]
fn stray_end_at_top_level_fails() {
    let error = parse_source("model A do\nend\nend").unwrap_err();
    assert_eq!((error.line, error.message.as_str()), (3, "expected `model`, found `end`"));
}

#[test]
fn trailing_comma_at_end_of_file_fails() {
    let error = parse_source("model A do\n  index :a,").unwrap_err();
    assert_eq!(error.message, "unexpected end of file");
}

#[test]
fn unexpected_token_in_arguments_fails() {
    let error = parse_source("model A do\n  index :a :b\nend").unwrap_err();
    assert_eq!((error.line, error.message.as_str()), (2, "unexpected `:b`"));
}

#[test]
fn unclosed_array_fails() {
    let error = parse_source("model A do\n  scope :a, params: [:x\nend").unwrap_err();
    assert_eq!(error.line, 3);
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
