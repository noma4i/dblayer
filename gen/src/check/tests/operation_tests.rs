use super::check_source;

fn run(rows: &[(&str, &str, Option<&str>)]) {
    for (name, body, expected) in rows {
        let source = format!("model Sample do\n  attribute :name, :string\n  attribute :ownerId, :id\n  attribute :clientKey, :string, null: true\n  attribute :group, Group, null: true do\n    attribute :position, :int\n    attribute :size, :int\n  end\n  scope :feed do\n    query \"Feed\"\n  end\n{body}\nend\n");
        match (check_source(&source), expected) {
            (Ok(_), None) => {}
            (Ok(_), Some(message)) => panic!("{name}: accepted, expected `{message}`"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some(message)) => assert!(errors.iter().any(|error| error.message.contains(message)), "{name}: {errors:?}"),
        }
    }
}

fn op(name: &str, lines: &str) -> String {
    format!("  operation :{name} do\n    mutation \"M\"\n{lines}  end")
}

fn custom(lines: &str) -> String {
    op("act", &format!("    optimistic false\n{lines}"))
}

#[test]
fn standard_operations_take_a_closed_set_of_lines() {
    run(&[
        ("create", &op("create", "    response from: \"a.b\"\n    match_by \"clientKey\"\n    local :ownerId\n    on_failure :keep\n    refetch :feed\n"), None),
        ("update", &op("update", "    variables sampleId: :id\n    on_failure :rollback\n"), None),
        ("destroy", &op("destroy", "    variables sampleId: :id\n    response from: \"a.b\"\n"), None),
        ("no mutation", "  operation :create do\n  end", Some("operation `create` needs a `mutation` line")),
        ("mutation is a constant", "  operation :create do\n    mutation CreateSample\n  end", Some("the operation name must be a string")),
        ("mutation twice", &op("create", "    mutation \"N\"\n"), Some("`mutation` is declared twice")),
        ("old with option", "  operation :create, with: Create do\n  end", Some("unknown key `with` for `operation`")),
        ("old input line", &op("create", "    input name: :name\n"), Some("unknown word `input` in an operation")),
        ("old args line", &op("create", "    args name: :string\n"), Some("unknown word `args` in an operation")),
        ("old root line", &op("create", "    root \"a.b\"\n"), Some("unknown word `root` in an operation")),
        ("old correlate line", &op("create", "    correlate :clientKey\n"), Some("unknown word `correlate` in an operation")),
        ("destroy takes no on_failure", &op("destroy", "    on_failure :keep\n"), Some("`on_failure` does not apply to operation `destroy`")),
        ("destroy takes no parameter", &op("destroy", "    parameter :x, :string\n"), Some("`parameter` does not apply to operation `destroy`")),
        ("destroy takes no match_by", &op("destroy", "    match_by \"clientKey\"\n"), Some("`match_by` does not apply to operation `destroy`")),
        ("create takes no optimistic", &op("create", "    optimistic false\n"), Some("`optimistic` does not apply to operation `create`")),
        ("create takes no upsert", &op("create", "    upsert Sample, from: \"a.b\"\n"), Some("`upsert` does not apply to operation `create`")),
        ("create takes no returns", &op("create", "    returns do\n      attribute :ok, :bool\n    end\n"), Some("`returns` does not apply to operation `create`")),
        ("create takes no batch", &op("create", "    batch items: \"items\", single: :act\n"), Some("`batch` does not apply to operation `create`")),
        ("update takes no upload", "  operation :update, upload: true do\n    mutation \"M\"\n  end", Some("`upload: true` belongs to a named operation")),
        ("a name used twice", &format!("{}\n{}", op("create", ""), op("create", "")), Some("name `create` is declared twice")),
    ]);
}

#[test]
fn parameters_and_variables_are_checked_against_attributes() {
    run(&[
        ("typed parameter", &op("create", "    parameter :token, :string, null: true\n"), None),
        ("parameter with an attribute name", &op("create", "    parameter :name, :string\n"), Some("parameter `name` has the name of an attribute")),
        ("parameter twice", &op("create", "    parameter :t, :string\n    parameter :t, :string\n"), Some("parameter `t` is declared twice")),
        ("parameter with a default", &op("create", "    parameter :t, :string, default: \"x\"\n"), Some("an operation parameter has no `default`")),
        ("parameter of an unknown type", &op("create", "    parameter :t, :date\n"), Some("parameter type must be")),
        ("variable names unknown source", &op("create", "    variables other: :ghost\n"), Some("variable `other` names unknown attribute or parameter `ghost`")),
        ("variable is a literal", &op("create", "    variables name: \"x\"\n"), Some("a variable is bound to an attribute or parameter symbol")),
        ("variables twice", &op("create", "    variables sampleId: :id\n    variables sampleId: :id\n"), Some("`variables` is declared twice")),
        ("list of ids", &custom("    parameter :ids, [:id]\n"), None),
        ("list of inputs with nullable items", &custom("    parameter :w, [WidgetInput], null_items: true\n"), None),
        ("null_items on a scalar", &custom("    parameter :w, :string, null_items: true\n"), Some("`null_items` applies to a list of schema inputs or enums")),
        ("local names unknown attribute", &op("create", "    local :ghost\n"), Some("`local` names unknown attribute `ghost`")),
        ("local without names", &op("create", "    local\n"), Some("`local` needs 1 or more attribute names")),
        ("response ignore", &op("create", "    response :ignore\n"), None),
        ("response ignore with match_by", &op("create", "    response :ignore\n    match_by \"clientKey\"\n"), Some("ignores its response")),
        ("response from and ignore", &op("create", "    response :ignore\n    response from: \"a.b\"\n"), Some("`response` is declared twice")),
        ("response unknown symbol", &op("create", "    response :keep\n"), Some("`response` is `response :ignore`")),
        ("match_by path with empty segment", &op("create", "    match_by \"a..b\"\n"), Some("has an empty segment")),
        ("match_by symbol", &op("create", "    match_by :clientKey\n"), Some("a `match_by` path must be a string")),
        ("match_by without paths", &op("create", "    match_by\n"), Some("`match_by` needs 1 or more paths")),
        ("on_failure unknown", &op("create", "    on_failure :ignore\n"), Some("`on_failure` must be `:keep` or `:rollback`")),
        ("refetch unknown scope", &op("create", "    refetch :ghost\n"), Some("`refetch` names `ghost`, which is not a scope with `query`")),
        ("refetch unknown model", &op("create", "    refetch :feed, model: Ghost\n"), Some("`refetch` names unknown model `Ghost`")),
        ("refetch twice as a set", &op("create", "    refetch :feed\n    refetch :feed\n"), Some("`refetch :feed` is declared twice")),
        ("refetch order does not matter", &custom("    refetch :feed\n    refetch :feed, model: Other\n"), Some("`refetch` names unknown model `Other`")),
    ]);
}

#[test]
fn a_named_operation_declares_its_local_effect_explicitly() {
    run(&[
        ("optimistic false", &custom(""), None),
        ("optimistic values", &op("act", "    optimistic name: \"done\"\n"), None),
        ("old local_effect", &op("act", "    local_effect :none, reason: \"x\"\n"), Some("unknown word `local_effect` in an operation")),
        ("optimistic true", &op("act", "    optimistic true\n"), Some("`optimistic` is `optimistic false`")),
        ("optimistic unknown attribute", &op("act", "    optimistic ghost: 1\n"), Some("`optimistic` names unknown attribute `ghost`")),
        ("optimistic from a parameter", &op("act", "    parameter :done, :bool\n    optimistic name: :done\n"), None),
        ("optimistic undeclared parameter", &op("act", "    optimistic name: :done\n"), Some("`optimistic` value `:done` is not declared with `parameter`")),
        ("optimistic keyed", &op("act", "    parameter :label, :string\n    optimistic group: { by: :position, element: { position: :label, size: nil } }\n"), None),
        ("optimistic map", &op("act", "    parameter :decision, Decision\n    optimistic name: { from: :decision, map: { ACCEPT: \"yes\" } }\n"), None),
        ("optimistic map of a plain parameter", &op("act", "    parameter :decision, :string\n    optimistic name: { from: :decision, map: { ACCEPT: \"yes\" } }\n"), Some("must be a schema enum parameter")),
        ("optimistic map of an undeclared parameter", &op("act", "    optimistic name: { from: :decision, map: { ACCEPT: \"yes\" } }\n"), Some("`optimistic` map reads `:decision`, which is not declared")),
        ("optimistic map without map", &op("act", "    optimistic name: { from: :decision }\n"), Some("optimistic hash must be `{ from:")),
        ("optimistic keyed without by", &op("act", "    optimistic group: { element: { position: 1 } }\n"), Some("optimistic hash must be")),
        ("optimistic keyed key outside the element", &op("act", "    optimistic group: { by: :position, element: { size: 1 } }\n"), Some("with the key among the element fields")),
        ("optimistic list literal", &op("act", "    optimistic name: [1]\n"), Some("optimistic value must be a literal or a parameter symbol")),
        ("optimistic twice", &op("act", "    optimistic false\n    optimistic false\n"), Some("`optimistic` is declared twice")),
        ("no mutation", "  operation :act do\n    optimistic false\n  end", Some("needs a `mutation` line")),
        ("once is not a word", &custom("    once\n"), Some("unknown word `once` in an operation")),
        ("reason is not a key", &op("act", "    optimistic false, reason: \"x\"\n"), Some("`optimistic` is `optimistic false`")),
        ("on_failure", &custom("    on_failure :keep\n"), None),
    ]);
}

#[test]
fn upsert_and_returns_belong_to_named_operations() {
    run(&[
        ("upsert", &custom("    upsert Sample, from: \"a.b\"\n    upsert Sample, from: \"a.c\"\n"), None),
        ("upsert unknown model", &custom("    upsert Ghost, from: \"a.b\"\n"), Some("`upsert` names unknown model `Ghost`")),
        ("upsert without from", &custom("    upsert Sample\n"), Some("`upsert` needs `from:`")),
        ("upsert without a model", &custom("    upsert from: \"a.b\"\n"), Some("`upsert` needs a model")),
        ("old lands", &custom("    lands Sample, root: \"a.b\"\n"), Some("unknown word `lands` in an operation")),
        ("returns scalars", &custom("    returns do\n      attribute :ok, :bool\n      attribute :note, :string\n    end\n"), None),
        ("returns an object", &custom("    returns do\n      attribute :thing, Thing do\n        attribute :x, :int\n      end\n    end\n"), None),
        ("returns null is implicit", &custom("    returns do\n      attribute :ok, :bool, null: true\n    end\n"), Some("every returned attribute is nullable")),
        ("returns empty", &custom("    returns do\n    end\n"), Some("`returns` block needs at least 1 attribute")),
        ("returns without a block", &custom("    returns\n"), Some("`returns` needs a `do ... end` block")),
        ("old result", &custom("    result ok: :bool\n"), Some("unknown word `result` in an operation")),
        ("returns twice", &custom("    returns do\n      attribute :ok, :bool\n    end\n    returns do\n      attribute :ok, :bool\n    end\n"), Some("`returns` is declared twice")),
    ]);
}

#[test]
fn batch_and_validates_belong_together_and_only_to_uploads() {
    let single = "  operation :one, upload: true do\n    mutation \"One\"\n    optimistic false\n    match_by \"clientKey\"\n  end\n";
    let many = |lines: &str| format!("{single}  operation :many, upload: true do\n    mutation \"Many\"\n    optimistic false\n    match_by \"clientKey\"\n{lines}  end");
    let batch = "    batch items: \"items\", single: :one, input: { clientKey: :clientKey }, renumber: { position: \"group.position\", size: \"group.size\", start: 0 }\n";
    let length = "    validates :items, length: { minimum: 2, maximum: 10 }\n";
    run(&[
        ("batch with validates", &many(&format!("{batch}{length}")), None),
        ("batch without renumber", &many(&format!("    batch items: \"items\", single: :one, input: {{ clientKey: :clientKey }}\n{length}")), None),
        ("renumber starts at 1", &many(&format!("{}{length}", batch.replace("start: 0", "start: 1"))), None),
        ("item outside a batch", &many("    item clientKey: :clientKey\n"), Some("unknown word `item` in an operation")),
        ("renumber outside a batch", &many("    renumber position: \"group.position\"\n"), Some("unknown word `renumber` in an operation")),
        ("old min and max", &many("    batch min: 2, max: 10, items: \"items\", single: :one\n"), Some("unknown key `min` for `batch`")),
        ("batch without validates", &many(batch), Some("`batch` needs `validates :items, length:")),
        ("validates without batch", &many(length), Some("`validates` needs a `batch`")),
        ("validates names another variable", &many(&format!("{batch}    validates :rows, length: {{ minimum: 2, maximum: 10 }}\n")), Some("`validates :rows` must name the batch items `items`")),
        ("validates without length", &many(&format!("{batch}    validates :items\n")), Some("`validates` needs `length:`")),
        ("validates length without maximum", &many(&format!("{batch}    validates :items, length: {{ minimum: 2 }}\n")), Some("`length` needs `maximum:`")),
        ("validates length key unknown", &many(&format!("{batch}    validates :items, length: {{ minimum: 2, maximum: 3, is: 2 }}\n")), Some("unknown key `is` for `length`")),
        ("length below 2", &many(&format!("{batch}    validates :items, length: {{ minimum: 1, maximum: 10 }}\n")), Some("2 <= minimum <= maximum <= 10")),
        ("length above 10", &many(&format!("{batch}    validates :items, length: {{ minimum: 2, maximum: 11 }}\n")), Some("2 <= minimum <= maximum <= 10")),
        ("batch single is unknown", &many(&format!("{}{length}", batch.replace("single: :one", "single: :ghost"))), Some("`single:` must name an operation with `upload: true`")),
        ("batch on a non upload", &op("act", &format!("    optimistic false\n{batch}{length}")), Some("`batch` belongs to an operation with `upload: true`")),
        ("batch without items", &many(&format!("    batch single: :one\n{length}")), Some("`batch` needs `items:`")),
        ("batch without single", &many(&format!("    batch items: \"items\"\n{length}")), Some("`batch` needs `single:`")),
        ("batch items is a symbol", &many(&format!("    batch items: :items, single: :one\n{length}")), Some("`items` must be a string")),
        ("renumber start 2", &many(&format!("{}{length}", batch.replace("start: 0", "start: 2"))), Some("renumber `start` must be 0 or 1")),
        ("renumber without start", &many(&format!("{}{length}", batch.replace(", start: 0", ""))), Some("missing start")),
        ("renumber position equals size", &many(&format!("{}{length}", batch.replace("group.size", "group.position"))), Some("name the same field")),
        ("renumber key unknown", &many(&format!("{}{length}", batch.replace("start: 0", "start: 0, base: 1"))), Some("unknown key `base` for `renumber`")),
        ("batch twice", &many(&format!("{batch}{batch}{length}")), Some("`batch` is declared twice")),
        ("upload false", "  operation :one, upload: false do\n    mutation \"M\"\n    optimistic false\n  end", Some("`upload` is only `true`")),
    ]);
}

#[test]
fn optimistic_include_and_at_least_follow_a_bool_parameter() {
    let source = |lines: &str| format!("model Viewer do\n  attribute :name, :string\n  scope :current do\n    query \"Viewer\"\n  end\nend\n\nmodel Post do\n  attribute :reacted, :bool\n  attribute :visitorIds, [:id]\n  attribute :visitsCount, :int\n  attribute :title, :string\n{}\nend\n", op("react", lines));
    let head = "    parameter :reacted, :bool\n";
    let both = format!("{head}    optimistic reacted: :reacted, visitorIds: {{ include: {{ of: Viewer, field: :id }}, when: :reacted }}, visitsCount: {{ at_least: 1, when: :reacted }}\n");
    let models = check_source(&source(&both)).unwrap();
    let action = &serde_json::to_value(&models[1]).unwrap()["actions"][0]["optimistic"];
    assert_eq!(action["visitsCount"], serde_json::json!({"$flag": {"when": "reacted", "at_least": 1}}));
    assert_eq!(action["visitorIds"], serde_json::json!({"$flag": {"when": "reacted", "include": {"of": "Viewer", "row": null, "field": "id"}}}));
    for (lines, message) in [
        (format!("{head}    optimistic visitorIds: {{ include: {{ of: Viewer, field: :id }}, when: :ghost }}\n"), "`:ghost`, which is not declared"),
        ("    parameter :reacted, :string\n    optimistic visitsCount: { at_least: 1, when: :reacted }\n".to_string(), "must be a bool parameter"),
        (format!("{head}    optimistic title: {{ at_least: 1, when: :reacted }}\n"), "needs `title` to be a number attribute"),
        (format!("{head}    optimistic title: {{ include: {{ of: Viewer, field: :id }}, when: :reacted }}\n"), "needs `title` to be a list attribute"),
        (format!("{head}    optimistic visitorIds: {{ include: {{ of: Ghost, field: :id }}, when: :reacted }}\n"), "names unknown model `Ghost`"),
        (format!("{head}    optimistic visitsCount: {{ at_least: 1.5, when: :reacted }}\n"), "`at_least` takes a whole number"),
        (format!("{head}    optimistic visitsCount: {{ at_least: 1 }}\n"), "optimistic hash is `{ include:"),
    ] {
        let errors = check_source(&source(&lines)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(message)), "{lines}: {errors:?}");
    }
}
