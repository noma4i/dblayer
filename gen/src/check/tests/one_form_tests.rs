use crate::parse_models;

const QUERY_SCOPE: &str = "  scope :recent do\n    query \"RecentItems\"\n  end\n";

fn model(body: &str) -> String { format!("model Item do\n  attribute :id, :id\n  attribute :title, :string, null: true\n  attribute :done, :bool, null: true\n{body}end\n") }

fn refused(source: &str, expected: &str) -> Option<String> {
    match parse_models(source) {
        Ok(_) => Some(format!("accepted, wanted an error containing `{expected}`:\n{source}")),
        Err(errors) if errors.iter().any(|error| error.message.contains(expected)) => None,
        Err(errors) => Some(format!("wanted `{expected}`, got {errors:?}")),
    }
}

/// D-482: a query scope without `freshness` keeps rows 5 minutes; `freshness` is written only to change that.
#[test]
fn a_query_scope_without_freshness_keeps_rows_five_minutes() {
    let models = parse_models(&model(QUERY_SCOPE)).unwrap_or_else(|errors| panic!("{errors:?}"));
    let value = serde_json::to_value(&models[0]).unwrap();
    assert_eq!(value["relations"][0]["remote"]["stale_time"], "5.minutes");
}

/// D-482: an operation's queue lifetime is a duration like every other lifetime of the DSL.
#[test]
fn an_operation_lifetime_is_a_duration() {
    let operation = |lifetime: &str| model(&format!("  operation :finish, expires_after: {lifetime} do\n    mutation \"ItemFinish\"\n    optimistic false\n  end\n"));
    let models = parse_models(&operation("2.hours")).unwrap_or_else(|errors| panic!("{errors:?}"));
    let value = serde_json::to_value(&models[0]).unwrap();
    assert_eq!(value["actions"][0]["expires_after"], 7200, "2.hours is 7200 seconds");
    assert_eq!(refused(&operation("7200"), "duration"), None);
}

/// D-482: one form per question: each other spelling stops the generator with its line.
#[test]
fn every_second_spelling_stops_the_generator() {
    let memory = |body: &str| format!("model Item, storage: :memory do\n  attribute :id, :id\n{body}end\n");
    let cases: Vec<(String, &str)> = vec![
        (model("  scope :recent, keep_on_gap: false do\n    query \"RecentItems\"\n  end\n"), "only `true`"),
        (model("  scope :recent, stable_order: false do\n    query \"RecentItems\"\n  end\n"), "only `true`"),
        (memory("  scope :recent, keep_on_gap: true do\n    subscribe \"ItemSignal\", from: \"itemSignal\", id: \"id\", apply: { effect: :upsert }\n  end\n"), "needs `query`"),
        (model("  attribute :note, :string, local: true, default: \"x\"\n"), "`local`"),
        (model("  attribute :note, :string, local: true, merge: :present\n"), "`local`"),
        (model("  index :title\n  index :title\n"), "declared twice"),
        (model("  belongs_to :owner, class_name: Item, foreign_key: :title\n  belongs_to :owner, class_name: Item, foreign_key: :title\n"), "declared twice"),
        (model("  operation :finish, upload: true do\n    mutation \"ItemFinish\"\n    processing \"status\", done: [\"READY\"]\n  end\n"), "processing"),
    ];
    let missed: Vec<String> = cases.iter().filter_map(|(source, expected)| refused(source, expected)).collect();
    assert!(missed.is_empty(), "{missed:#?}");
}
