use super::*;

fn kinds(source: &str) -> Vec<Kind> {
    lex(source).unwrap().into_iter().map(|token| token.kind).collect()
}

fn word(text: &str) -> Kind {
    Kind::Word(text.to_string())
}

fn symbol(text: &str) -> Kind {
    Kind::Symbol(text.to_string())
}

#[test]
fn empty_source_has_no_tokens() {
    assert_eq!(kinds(""), vec![]);
    assert_eq!(kinds("   \n\n  "), vec![Kind::Newline, Kind::Newline]);
}

#[test]
fn field_line_has_word_symbol_and_options() {
    assert_eq!(
        kinds("string :uuid, null: true, default: nil"),
        vec![
            word("string"),
            symbol("uuid"),
            Kind::Comma,
            word("null"),
            Kind::Colon,
            Kind::Bool(true),
            Kind::Comma,
            word("default"),
            Kind::Colon,
            Kind::Nil,
        ]
    );
}

#[test]
fn constants_strings_and_numbers() {
    assert_eq!(
        kinds("model: User, \"a b\", 20, 1.5, -3"),
        vec![
            word("model"),
            Kind::Colon,
            Kind::Const("User".into()),
            Kind::Comma,
            Kind::Str("a b".into()),
            Kind::Comma,
            Kind::Int(20),
            Kind::Comma,
            Kind::Float(1.5),
            Kind::Comma,
            Kind::Int(-3),
        ]
    );
}

#[test]
fn durations_keep_amount_and_unit() {
    assert_eq!(
        kinds("5.minutes 1.day 30.seconds"),
        vec![
            Kind::Duration { amount: 5, unit: "minutes".into() },
            Kind::Duration { amount: 1, unit: "day".into() },
            Kind::Duration { amount: 30, unit: "seconds".into() },
        ]
    );
}

#[test]
fn brackets_braces_and_hash_pairs() {
    assert_eq!(
        kinds("order: { createdAt: :desc }, params: [:a, :b]"),
        vec![
            word("order"),
            Kind::Colon,
            Kind::LeftBrace,
            word("createdAt"),
            Kind::Colon,
            symbol("desc"),
            Kind::RightBrace,
            Kind::Comma,
            word("params"),
            Kind::Colon,
            Kind::LeftBracket,
            symbol("a"),
            Kind::Comma,
            symbol("b"),
            Kind::RightBracket,
        ]
    );
}

#[test]
fn comments_are_dropped_and_lines_split() {
    assert_eq!(kinds("# only\nend # tail\nend"), vec![Kind::Newline, word("end"), Kind::Newline, word("end")]);
}

#[test]
fn newline_after_comma_is_continuation() {
    assert_eq!(
        kinds("scope :a,\n  where: 1"),
        vec![word("scope"), symbol("a"), Kind::Comma, word("where"), Kind::Colon, Kind::Int(1)]
    );
    assert_eq!(kinds("a, # note\n\n b"), vec![word("a"), Kind::Comma, word("b")]);
}

#[test]
fn tokens_carry_line_and_column() {
    let tokens = lex("do\n  end").unwrap();
    assert_eq!((tokens[0].line, tokens[0].column), (1, 1));
    assert_eq!((tokens[2].line, tokens[2].column), (2, 3));
}

#[test]
fn hash_inside_string_is_text() {
    assert_eq!(kinds("\"a # b\""), vec![Kind::Str("a # b".into())]);
}

#[test]
fn lexical_errors_report_line_column_and_message() {
    let cases = [
        ("unterminated string", "a\nb \"oops", 2, None, "unterminated string"),
        ("unknown character", "a @", 1, Some(3), "unexpected character `@`"),
        ("unknown duration unit", "fresh_for 5.parsecs", 1, None, "unknown duration unit `parsecs`"),
    ];
    for (case, source, line, column, message) in cases {
        let error = lex(source).unwrap_err();
        assert_eq!((error.line, error.message.as_str()), (line, message), "{case}");
        if let Some(column) = column {
            assert_eq!(error.column, column, "{case}: column");
        }
    }
}

#[test]
fn bare_colon_without_name_is_colon() {
    assert_eq!(kinds("a: b"), vec![word("a"), Kind::Colon, word("b")]);
}

#[test]
fn a_symbol_may_end_with_a_question_mark() {
    assert_eq!(kinds("a: :id?, b: :string"), vec![word("a"), Kind::Colon, symbol("id?"), Kind::Comma, word("b"), Kind::Colon, symbol("string")]);
}
