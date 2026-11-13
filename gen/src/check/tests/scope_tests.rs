use super::check_source;

fn run(rows: &[(&str, &str, Option<&str>)]) {
    for (name, body, expected) in rows {
        let source = format!("model Sample do\n  attribute :name, :string\n  attribute :ownerId, :id\n  attribute :createdAt, :date\n  index :ownerId, :createdAt\n  attribute :count, :int, null: true\n{body}\nend\n");
        match (check_source(&source), expected) {
            (Ok(_), None) => {}
            (Ok(_), Some(message)) => panic!("{name}: accepted, expected `{message}`"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some(message)) => assert!(errors.iter().any(|error| error.message.contains(message)), "{name}: {errors:?}"),
        }
    }
}

const BY_OWNER: &str = "    parameter :ownerId, :id\n    where ownerId: :ownerId\n";

fn scope(options: &str, lines: &str) -> String {
    format!("  scope :s{options} do\n{lines}  end")
}

#[test]
fn a_local_scope_takes_parameters_where_and_order() {
    run(&[
        ("global with order", &scope("", "    order createdAt: :desc\n"), None),
        ("where", &scope("", &format!("{BY_OWNER}    order createdAt: :asc\n")), None),
        ("scope without a block", "  scope :s", Some("`scope` needs a `do ... end` block")),
        ("scope name is a string", "  scope \"s\" do\n  end", Some("scope name must be a symbol")),
        ("scope option unknown", &scope(", ordered: true", ""), Some("unknown key `ordered` for `scope`")),
        ("old params option", "  scope :s, params: [:ownerId] do\n  end", Some("unknown key `params` for `scope`")),
        ("old text option", "  scope :s, text: [:q] do\n  end", Some("unknown key `text` for `scope`")),
        ("old where positional", &scope("", "    where :ownerId\n"), Some("`where` takes no arguments")),
        ("where without pairs", &scope("", "    where\n"), Some("`where` needs pairs")),
        ("where names an undeclared parameter", &scope("", "    where ownerId: :ghost\n"), Some("where value `:ghost` is not declared with `parameter`")),
        ("where value is a string", &scope("", "    parameter :ownerId, :id\n    where ownerId: \"ownerId\"\n"), Some("`where` value must be a symbol")),
        ("where twice", &scope("", &format!("{BY_OWNER}    where ownerId: :ownerId\n")), Some("`where` is declared twice")),
        ("parameter not used by where", &scope("", "    parameter :ownerId, :id\n"), Some("parameter `:ownerId` is not used in `where`")),
        ("parameter twice", &scope("", &format!("{BY_OWNER}    parameter :ownerId, :id\n")), Some("parameter `ownerId` is declared twice")),
        ("parameter of the wrong type", &scope("", "    parameter :ownerId, :int\n    where ownerId: :ownerId\n"), Some("is :int but column `ownerId` is a different type")),
        ("where parameter with a default", &scope("", "    parameter :ownerId, :id, default: \"x\"\n    where ownerId: :ownerId\n"), Some("filters a column")),
        ("order direction", &scope("", "    order createdAt: :up\n"), Some("order direction must be")),
        ("order without pairs", &scope("", "    order\n"), Some("`order` needs pairs")),
        ("order positional", &scope("", "    order :createdAt\n"), Some("`order` takes no arguments")),
        ("order column twice", &scope("", "    order createdAt: :asc, createdAt: :desc\n"), Some("order column `createdAt` is given twice")),
        ("order null placement", &scope("", "    order count: :asc_nulls_last, createdAt: :desc_nulls_first\n"), None),
        ("query in a local scope is the remote form", &scope("", "    query \"S\"\n"), None),
        ("freshness needs a query", &scope("", "    freshness expires_in: 1.minute\n"), Some("`freshness` needs a scope with `query`")),
        ("old fetch", &scope("", "    fetch \"S\"\n"), Some("`fetch` needs a scope with `query`")),
        ("a word twice", &scope("", "    order createdAt: :asc\n    order createdAt: :desc\n"), Some("`order` is declared twice")),
    ]);
}

#[test]
fn a_remote_scope_reads_with_query_variables_and_response() {
    run(&[
        ("query alone", &scope("", "    query \"S\"\n"), None),
        ("query is a constant", &scope("", "    query S\n"), Some("the operation name must be a string")),
        ("query twice", &scope("", "    query \"S\"\n    query \"T\"\n"), Some("`query` is declared twice")),
        ("query option", &scope("", "    query \"S\", root: \"a.b\"\n"), Some("unknown key `root` for `query`")),
        ("variables with a parameter and a constant", &scope("", "    parameter :q, :string\n    query \"S\"\n    variables query: :q, limit: 5, kinds: [\"a\"]\n"), None),
        ("cross binding reads original parameters", &scope("", "    parameter :a, :string\n    parameter :b, :string\n    query \"S\"\n    variables a: :b, b: :a\n"), None),
        ("variables names an undeclared parameter", &scope("", "    query \"S\"\n    variables query: :q\n"), Some("variable `query` names `:q`")),
        ("variables without bindings", &scope("", "    query \"S\"\n    variables\n"), Some("`variables` needs bindings")),
        ("variable bound twice", &scope("", "    parameter :q, :string\n    query \"S\"\n    variables a: :q, a: :q\n"), Some("variable `a` is bound twice")),
        ("variable bound to a type name", &scope("", "    query \"S\"\n    variables a: Thing\n"), Some("a variable is bound to a parameter symbol")),
        ("response path and list", &scope("", "    query \"S\"\n    response from: \"a.b\", shape: :list\n"), None),
        ("response shape one", &scope("", "    query \"S\"\n    response shape: :one\n"), None),
        ("response without options", &scope("", "    query \"S\"\n    response\n"), Some("`response` needs `from:` or `shape:`")),
        ("response shape unknown", &scope("", "    query \"S\"\n    response shape: :many\n"), Some("`shape` must be `:one` or `:list`")),
        ("response ignore", &scope("", "    query \"S\"\n    response :ignore\n"), Some("a scope reads its response")),
        ("response from empty segment", &scope("", "    query \"S\"\n    response from: \"a.\"\n"), Some("has an empty segment")),
        ("old list word", &scope("", "    query \"S\"\n    list\n"), Some("unknown word `list` in a scope")),
        ("old root option", &scope("", "    query \"S\"\n    root \"a\"\n"), Some("unknown word `root` in a scope")),
    ]);
}

#[test]
fn server_only_parameters_have_a_type_and_a_typed_default() {
    run(&[
        ("string", &scope("", "    parameter :q, :string\n    query \"S\"\n"), None),
        ("id", &scope("", "    parameter :q, :id\n    query \"S\"\n"), None),
        ("int", &scope("", "    parameter :q, :int\n    query \"S\"\n"), None),
        ("bool", &scope("", "    parameter :q, :bool\n    query \"S\"\n"), None),
        ("enum name", &scope("", "    parameter :q, Kind\n    query \"S\"\n"), None),
        ("enum default", &scope("", "    parameter :q, Kind, default: \"A\"\n    query \"S\"\n"), None),
        ("date is not a parameter type here", &scope("", "    parameter :q, :date\n    query \"S\"\n"), Some("parameter type must be")),
        ("num is not a server-only type", &scope("", "    parameter :q, :num\n    query \"S\"\n"), Some("a server-only parameter is")),
        ("list is not a server-only type", &scope("", "    parameter :q, [:id]\n    query \"S\"\n"), Some("a server-only parameter is")),
        ("parameter without a type", &scope("", "    parameter :q\n    query \"S\"\n"), Some("`parameter` needs a name and a type")),
        ("int default is refused, not dropped", &scope("", "    parameter :q, :int, default: 5\n    query \"S\"\n"), Some("does not match its type")),
        ("string default for a bool", &scope("", "    parameter :q, :bool, default: \"yes\"\n    query \"S\"\n"), Some("does not match its type")),
        ("string default for a string", &scope("", "    parameter :q, :string, default: \"x\"\n    query \"S\"\n"), Some("a default belongs to an enum parameter")),
        ("number default for a string", &scope("", "    parameter :q, :string, default: 3\n    query \"S\"\n"), Some("does not match its type")),
        ("null on a server-only parameter", &scope("", "    parameter :q, :string, null: true\n    query \"S\"\n"), None),
        ("parameter option unknown", &scope("", "    parameter :q, :string, optional: true\n    query \"S\"\n"), Some("unknown key `optional` for `parameter`")),
    ]);
}

#[test]
fn paginate_is_one_declaration_with_a_whole_cursor_or_none() {
    let paged = |line: &str| scope("", &format!("    query \"S\"\n    {line}\n"));
    run(&[
        ("size only", &paged("paginate size: 20"), None),
        ("forward", &paged("paginate size: 20, direction: :forward, max_pages: 3"), None),
        ("backward", &paged("paginate size: 20, direction: :backward"), None),
        ("whole cursor", &paged("paginate size: 20, cursor: { parameter: \"after\", from: \"page.last\", type: :int }"), None),
        ("string cursor", &paged("paginate size: 20, cursor: { parameter: \"after\", from: \"page.last\", type: :string }"), None),
        ("old positional size", &paged("paginate 20"), Some("`paginate` takes no arguments")),
        ("old backward flag", &paged("paginate size: 20, backward: true"), Some("unknown key `backward` for `paginate`")),
        ("size missing", &paged("paginate direction: :forward"), Some("`paginate` needs `size:`")),
        ("size zero", &paged("paginate size: 0"), Some("`size` must be a positive number")),
        ("direction unknown", &paged("paginate size: 20, direction: :sideways"), Some("`direction` must be `:forward` or `:backward`")),
        ("direction is a boolean", &paged("paginate size: 20, direction: true"), Some("`direction` must be")),
        ("max_pages zero", &paged("paginate size: 20, max_pages: 0"), Some("`max_pages` must be a positive number")),
        ("cursor without from", &paged("paginate size: 20, cursor: { parameter: \"after\", type: :int }"), Some("missing from")),
        ("cursor without type", &paged("paginate size: 20, cursor: { parameter: \"after\", from: \"a.b\" }"), Some("missing type")),
        ("cursor without parameter", &paged("paginate size: 20, cursor: { from: \"a.b\", type: :int }"), Some("missing parameter")),
        ("cursor type unknown", &paged("paginate size: 20, cursor: { parameter: \"after\", from: \"a.b\", type: :bogus }"), Some("cursor `type` must be `:string` or `:int`")),
        ("cursor key unknown", &paged("paginate size: 20, cursor: { parameter: \"after\", from: \"a.b\", type: :int, extra: 1 }"), Some("unknown key `extra` for `cursor`")),
        ("cursor is a symbol", &paged("paginate size: 20, cursor: :after"), Some("`cursor` must be a hash")),
        ("paginate twice", &paged("paginate size: 20\n    paginate size: 10"), Some("`paginate` is declared twice")),
        ("paginate and list", &paged("paginate size: 20\n    response shape: :list"), Some("`paginate` and `response shape: :list` cannot be used together")),
        ("total", &paged("paginate size: 20\n    total from: \"page.count\""), None),
        ("total without paginate", &scope("", "    query \"S\"\n    total from: \"page.count\"\n"), Some("`total` needs `paginate`")),
        ("total without from", &paged("paginate size: 20\n    total"), Some("`total` needs `from:`")),
        ("edge", &paged("paginate size: 20\n    edge do\n      attribute :impression, :string, null: true\n    end"), None),
        ("edge without paginate", &scope("", "    query \"S\"\n    edge do\n      attribute :impression, :string, null: true\n    end\n"), Some("edge `impression` needs `paginate` in its scope")),
        ("edge without null", &paged("paginate size: 20\n    edge do\n      attribute :impression, :string\n    end"), Some("needs `null: true` or `optional: true`")),
        ("edge with a default", &paged("paginate size: 20\n    edge do\n      attribute :impression, :string, null: true, default: \"x\"\n    end"), Some("cannot set `default`")),
        ("edge twice", &paged("paginate size: 20\n    edge do\n      attribute :impression, :string, null: true\n    end\n    edge do\n      attribute :other, :string, null: true\n    end"), Some("`edge` is declared twice")),
        ("edge name twice", &paged("paginate size: 20\n    edge do\n      attribute :impression, :string, null: true\n      attribute :impression, :int, null: true\n    end"), Some("edge `impression` is declared twice")),
        ("old edge field form", &paged("paginate size: 20\n    edge :impression, :string"), Some("`edge` takes no arguments")),
    ]);
}

#[test]
fn freshness_is_one_declaration_with_durations() {
    let fresh = |line: &str| scope("", &format!("    query \"S\"\n    {line}\n"));
    run(&[
        ("expires", &fresh("freshness expires_in: 5.minutes"), None),
        ("expires overflows milliseconds", &fresh("freshness expires_in: 18446744073709551615.days"), Some("duration")),
        ("expires and empty", &fresh("freshness expires_in: 5.minutes, empty_in: 30.seconds"), None),
        ("old fresh_for", &fresh("fresh_for 5.minutes"), Some("unknown word `fresh_for`")),
        ("expires missing", &fresh("freshness empty_in: 30.seconds"), Some("`freshness` needs `expires_in:`")),
        ("expires is a number", &fresh("freshness expires_in: 5"), Some("`expires_in` must be a duration")),
        ("empty_in is a number", &fresh("freshness expires_in: 5.minutes, empty_in: 5"), Some("`empty_in` must be a duration")),
        ("old empty key", &fresh("freshness expires_in: 5.minutes, empty: 30.seconds"), Some("unknown key `empty` for `freshness`")),
        ("freshness twice", &fresh("freshness expires_in: 1.minute\n    freshness expires_in: 2.minutes"), Some("`freshness` is declared twice")),
    ]);
}

#[test]
fn insert_on_is_a_closed_set_of_sources() {
    let joined = |options: &str, lines: &str| scope(options, &format!("{lines}    query \"S\"\n"));
    let node_read = |options: &str| format!("  scope :node, node: :one do\n    parameter :id, :id\n    query \"N\"\n    variables id: :id\n  end\n{}", joined(options, BY_OWNER));
    run(&[
        ("create", &joined(", insert_on: [:create]", ""), None),
        ("event and response", &joined(", insert_on: [:event, :response]", BY_OWNER), None),
        ("event node", &node_read(", insert_on: [:event, :response, :node]"), None),
        ("node", &node_read(", insert_on: [:node]"), None),
        ("node without the node read of the model", &joined(", insert_on: [:node]", BY_OWNER), Some("`insert_on` with `:node` needs the `node` scope of the model")),
        ("event node without the node read of the model", &joined(", insert_on: [:event, :response, :node]", BY_OWNER), Some("`insert_on` with `:node` needs the `node` scope of the model")),
        ("order does not matter", &joined(", insert_on: [:response, :event]", BY_OWNER), None),
        ("unknown source", &joined(", insert_on: [:bogus]", BY_OWNER), Some("`insert_on` takes `:create`, `:event` and `:response`")),
        ("empty list", &joined(", insert_on: []", BY_OWNER), Some("`insert_on` needs at least 1 source")),
        ("a source twice", &joined(", insert_on: [:event, :event]", BY_OWNER), Some("`insert_on` names a source twice")),
        ("not a list", &joined(", insert_on: :event", BY_OWNER), Some("`insert_on` must be a list")),
        ("old joins", &joined(", joins: :events", BY_OWNER), Some("unknown key `joins` for `scope`")),
        ("event alone is not a supported set", &joined(", insert_on: [:event]", BY_OWNER), Some("`insert_on` is `[:create]` or `[:event, :response]`")),
        ("create with event", &joined(", insert_on: [:create, :event]", BY_OWNER), Some("`insert_on` is `[:create]` or `[:event, :response]`")),
        ("events need where", &joined(", insert_on: [:event, :response]", ""), Some("needs `where`")),
        ("events with a server-only parameter", &joined(", insert_on: [:event, :response]", &format!("{BY_OWNER}    parameter :q, :string\n")), Some("server-only parameters has none")),
        ("create with a server-only parameter", &joined(", insert_on: [:create]", "    parameter :q, :string\n"), None),
    ]);
}

#[test]
fn refresh_on_foreground_needs_a_scope_with_query() {
    run(&[
        ("remote", &scope(", refresh_on_foreground: true", "    query \"S\"\n"), None),
        ("local", &scope(", refresh_on_foreground: true", "    order createdAt: :desc\n"), Some("needs a scope with `query`")),
        ("false", &scope(", refresh_on_foreground: false", "    order createdAt: :desc\n"), None),
        ("symbol", &scope(", refresh_on_foreground: :yes", "    order createdAt: :desc\n"), Some("`refresh_on_foreground` must be true or false")),
    ]);
}

#[test]
fn keep_on_gap_needs_a_query_and_no_foreground_refresh() {
    run(&[
        ("remote", &scope(", keep_on_gap: true", "    query \"S\"\n"), None),
        ("local", &scope(", keep_on_gap: true", "    order createdAt: :desc\n"), Some("scope `s`: `keep_on_gap` needs a `query`")),
        ("with foreground refresh", &scope(", keep_on_gap: true, refresh_on_foreground: true", "    query \"S\"\n"), Some("scope `s`: `keep_on_gap` and `refresh_on_foreground` contradict")),
    ]);
}

#[test]
fn membership_checks_parameter_fields_types_and_conflicts() {
    for (rule, expected) in [
        ("membership list: { contains: :lists }", None),
        ("membership list: { equals: :status }", None),
        ("membership list: { contains: :status }", Some("must match the type")),
        ("membership list: { equals: :lists }", Some("must match the type")),
        ("membership ghost: { contains: :lists }", Some("must match the type")),
        ("membership list: { contains: :missing }", Some("must match the type")),
        ("membership list: { contains: :lists }, list: { contains: :lists }", Some("declared twice")),
        ("membership list: { contains: :lists, equals: :status }", Some("exactly one")),
        ("membership list: { other: :lists }", Some("exactly one")),
        ("where status: :list\n    membership list: { contains: :lists }", Some("conflicts with where")),
    ] {
        let source = format!("model Chat do\n  attribute :status, :enum, values: [\"PRIMARY\",\"SECONDARY\"], as: Kind\n  attribute :lists, [:enum], values: [\"PRIMARY\",\"SECONDARY\"], as: Kind\n  index :status\n  scope :node, node: :one do\n    parameter :id, :id\n    query \"ChatNode\"\n    variables id: :id\n  end\n  scope :list, insert_on: [:event, :response, :node] do\n    parameter :list, Kind\n    {rule}\n    query \"Chats\"\n    response shape: :list\n  end\nend");
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("chat.dbl");
        std::fs::write(&file, source).unwrap();
        match (crate::compile_files(&[file], None, &[]), expected) {
            (Ok(_), None) => {},
            (Err(errors), Some(expected)) => assert!(errors.iter().any(|error| error.error.message.contains(expected)), "{rule}: {errors:?}"),
            (actual, expected) => panic!("{rule}: {actual:?}, expected {expected:?}"),
        }
    }
}

#[test]
fn stable_order_keeps_the_shown_order_of_the_rows_where_selects() {
    let stable = |options: &str, lines: &str| scope(options, &format!("{lines}    query \"S\"\n"));
    run(&[
        ("stable", &stable(", stable_order: true", &format!("{BY_OWNER}    order createdAt: :asc\n")), None),
        ("old option name", &stable(", order_stable: true", &format!("{BY_OWNER}    order createdAt: :asc\n")), Some("unknown key `order_stable` for `scope`")),
        ("stable without order", &stable(", stable_order: true", BY_OWNER), Some("needs `order` and `where`")),
        ("stable without where", &stable(", stable_order: true", "    order createdAt: :asc\n"), Some("needs `order` and `where`")),
        ("stable with a server-only parameter", &stable(", stable_order: true", &format!("{BY_OWNER}    order createdAt: :asc\n    parameter :q, :string\n")), Some("without server-only parameters")),
        ("stable false", &stable(", stable_order: false", ""), None),
        ("stable is a symbol", &stable(", stable_order: :yes", ""), Some("`stable_order` must be true or false")),
    ]);
}

#[test]
fn node_scopes_have_a_closed_shape() {
    let node = |lines: &str| scope(", node: :one", lines);
    let many = |lines: &str| scope(", node: :many", lines);
    run(&[
        ("one", &node("    parameter :id, :id\n    query \"N\"\n    variables id: :id\n    freshness expires_in: 1.minute, empty_in: 5.seconds\n"), None),
        ("many", &many("    query \"N\"\n"), None),
        ("many with a list response", &many("    query \"N\"\n    response shape: :list\n"), None),
        ("node value unknown", &scope(", node: :all", "    query \"N\"\n"), Some("`node` must be `:one` or `:many`")),
        ("one without a parameter", &node("    query \"N\"\n    variables id: :id\n"), Some("exactly one parameter, declared as `parameter :id, :id`")),
        ("one with a string parameter", &node("    parameter :id, :string\n    query \"N\"\n    variables id: :id\n"), Some("exactly one parameter")),
        ("one without variables", &node("    parameter :id, :id\n    query \"N\"\n"), Some("binds its parameter with `variables id: :id`")),
        ("one with where", &node("    parameter :id, :id\n    where ownerId: :id\n    query \"N\"\n    variables id: :id\n"), Some("`where` does not apply to a `node: :one` scope")),
        ("one with paginate", &node("    parameter :id, :id\n    query \"N\"\n    variables id: :id\n    paginate size: 5\n"), Some("`paginate` does not apply to a `node: :one` scope")),
        ("one with response", &node("    parameter :id, :id\n    query \"N\"\n    variables id: :id\n    response from: \"a\"\n"), Some("`response` does not apply to a `node: :one` scope")),
        ("many with a parameter", &many("    parameter :id, :id\n    query \"N\"\n"), Some("`parameter` does not apply to a `node: :many` scope")),
        ("many with variables", &many("    query \"N\"\n    variables a: 1\n"), Some("`variables` does not apply to a `node: :many` scope")),
        ("many with a one shape", &many("    query \"N\"\n    response shape: :one\n"), Some("a `node: :many` scope reads a list")),
        ("old node word", "  node SampleNode", Some("unknown model declaration `node`")),
        ("old nodes word", "  nodes SampleNodes", Some("unknown model declaration `nodes`")),
    ]);
}

#[test]
fn scope_names_are_unique_within_the_model() {
    run(&[
        ("two scopes", &format!("{}\n  scope :t do\n  end", scope("", "")), None),
        ("same name twice", &format!("{}\n{}", scope("", ""), scope("", "")), Some("name `s` is declared twice")),
    ]);
}

fn derived(lines: &str) -> String {
    format!("  scope :list do\n    query \"L\"\n  end\n  scope :on_channel do\n    parameter :channel, :string\n    query \"S\"\n{lines}  end")
}

#[test]
fn preload_lists_the_values_of_every_parameter_of_a_remote_scope() {
    let remote = |lines: &str| scope("", &format!("    query \"S\"\n{lines}"));
    let with_channel = |lines: &str| remote(&format!("    parameter :channel, :string\n{lines}"));
    run(&[
        ("scope without parameters", &remote("    preload\n"), None),
        ("one parameter", &with_channel("    preload channel: [\"a\", \"b\"]\n"), None),
        ("where parameter", &scope("", &format!("{BY_OWNER}    query \"S\"\n    preload ownerId: [\"1\", \"2\"]\n")), None),
        ("local scope", &scope("", "    preload\n"), Some("`preload` needs a scope with `query`")),
        ("value list missing", &with_channel("    preload\n"), Some("needs a list of values for parameter `channel`")),
        ("unknown parameter", &remote("    preload ghost: [\"a\"]\n"), Some("`preload` names `ghost`")),
        ("empty list", &with_channel("    preload channel: []\n"), Some("needs at least 1 value")),
        ("value of the wrong type", &with_channel("    preload channel: [1]\n"), Some("does not match the type")),
        ("duplicate value", &with_channel("    preload channel: [\"a\", \"a\"]\n"), Some("lists a value twice")),
        ("not a list", &with_channel("    preload channel: \"a\"\n"), Some("takes a list of values")),
        ("twice", &remote("    preload\n    preload\n"), Some("`preload` is declared twice")),
        ("rows of another scope", &derived("    preload channel: { from: \"Sample.list\" }, limit: 40\n"), None),
        ("derived without limit", &derived("    preload channel: { from: \"Sample.list\" }\n"), Some("needs a positive `limit`")),
        ("zero limit", &derived("    preload channel: { from: \"Sample.list\" }, limit: 0\n"), Some("must be a positive integer")),
        ("rows of an unknown model", &derived("    preload channel: { from: \"Ghost.list\" }, limit: 40\n"), Some("unknown model `Ghost`")),
        ("rows of an unknown scope", &derived("    preload channel: { from: \"Sample.ghost\" }, limit: 40\n"), Some("has no scope `ghost`")),
        ("rows of a scope with parameters", &derived("    preload channel: { from: \"Sample.on_channel\" }, limit: 40\n"), Some("must be a list without parameters")),
        ("rows for a number parameter", &derived("    parameter :number, :int\n    preload channel: { from: \"Sample.list\" }, number: { from: \"Sample.list\" }, limit: 40\n"), Some("takes row ids")),
        ("source without a model", &derived("    preload channel: { from: \"list\" }, limit: 40\n"), Some("names its source scope")),
        ("source with another key", &derived("    preload channel: { of: \"Sample.list\" }\n"), Some("takes a list of values or `{ from:")),
    ]);
}

#[test]
fn matches_checks_fields_literals_and_the_threshold_parameter() {
    let remote = |rule: &str| scope(", insert_on: [:event, :response]", &format!("{BY_OWNER}    parameter :after, :int\n    {rule}\n    query \"S\"\n"));
    run(&[
        ("literal set and threshold", &remote("matches name: { in: [\"a\", \"b\"] }, count: { greater_than: :after }"), None),
        ("literal equality", &remote("matches name: { equals: \"a\" }, count: { greater_than: :after }"), None),
        ("unknown field", &remote("matches ghost: { equals: \"a\" }"), Some("is not an attribute")),
        ("literal of another type", &remote("matches name: { equals: 1 }"), Some("must match the type")),
        ("threshold on a string parameter", &remote("matches count: { greater_than: :ownerId }"), Some("needs an `:int` parameter")),
        ("threshold on a string field", &remote("matches name: { greater_than: :after }"), Some("needs an `:int` parameter")),
        ("two tests for one field", &remote("matches name: { in: [\"a\"], equals: \"b\" }"), Some("exactly one")),
        ("empty set", &remote("matches name: { in: [] }"), Some("non-empty array")),
        ("field given twice", &remote("matches name: { equals: \"a\" }, name: { equals: \"b\" }"), Some("given twice")),
        ("local scope", &scope("", &format!("{BY_OWNER}    matches name: {{ equals: \"a\" }}\n")), Some("`matches` needs a scope with `query`")),
    ]);
}

#[test]
fn a_subscription_id_of_a_scope_with_variables_names_a_variable() {
    let remote = |subscribe: &str| scope("", &format!("    parameter :q, :string\n    query \"S\"\n    variables slug: :q\n    {subscribe}\n"));
    run(&[
        ("id names a variable", &remote("subscribe \"Updated\", id: \"slug\""), None),
        ("no id", &remote("subscribe \"Updated\""), None),
        ("id names no variable", &remote("subscribe \"Updated\", id: \"other\""), Some("`id: \"other\"` is no variable of `variables`")),
        ("one of two ids names no variable", &remote("subscribe \"Updated\", id: [\"slug\", \"other\"]"), Some("`id: \"other\"` is no variable of `variables`")),
        ("scope without variables keeps the event id", &scope("", "    parameter :q, :string\n    query \"S\"\n    subscribe \"Updated\", id: \"other\"\n"), None),
    ]);
}
