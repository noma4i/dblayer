use dblayer_gen::{parse_models, emit_swift, emit_kotlin};

#[test]
fn platform_models_offer_typed_writes_and_live_scopes() {
    let models = parse_models(r#"model Moment do
  attribute :text, :string
  attribute :userId, :id, null: true
  attribute :createdAt, :date
  ephemeral do
    attribute :selected, :bool, default: false
  end
  belongs_to :user
  scope :recent do
    order createdAt: :desc
  end
  operation :like do
    mutation "LikeMoment"
    optimistic text: "liked"
  end
  scope :feed do
    query "MomentsFeed"
    response shape: :list
  end
end

model User do
  attribute :name, :string
end
"#).unwrap();
    let moment = &models[0];
    assert_eq!(moment.associations[0].target.as_str(), "User");
    assert_eq!(moment.associations[0].foreign_key, "userId");
    assert!(moment.is_indexed("userId") && moment.is_indexed("createdAt"));
    assert_eq!(moment.actions[0].variables["momentId"], "id");
    assert_eq!(moment.relations[1].remote.as_ref().unwrap().operation, "MomentsFeed");
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    for token in ["struct Moment", "DBModel", "func create(", "func set(", "static var recent", "static var feed", "func like("] {
        assert!(swift.contains(token), "missing Swift {token}");
    }
    for token in ["ModelDecoder<Moment>", "data class Moment", "fun create(", "fun set(", "val recent", "val feed", "fun like(", "fun ids("] {
        assert!(kotlin.contains(token), "missing Kotlin {token}");
    }
}

#[test]
fn object_fields_generate_the_types_they_decode_to() {
    let models = parse_models("model Moment do\n  attribute :media, MomentMedia, null: true do\n    attribute :url, :string\n  end\n  attribute :widgets, [MomentWidget] do\n    attribute :kind, :string\n  end\nend\n").unwrap();
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(swift.contains("public let media: MomentMedia?"), "{swift}");
    assert!(swift.contains("public let widgets: [MomentWidget]"), "{swift}");
    assert!(swift.contains("Column<Moment, MomentMedia?>(\"media\")"), "{swift}");
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(kotlin.contains("val media: MomentMedia?"), "{kotlin}");
    assert!(kotlin.contains("val widgets: List<MomentWidget>"), "{kotlin}");
}

#[test]
fn kotlin_models_json_escapes_interpolation_markers() {
    let models = parse_models(r#"model Moment do
  attribute :liked, :bool
  operation :like do
    mutation "LikeMoment"
    parameter :liked, :bool
    variables liked: :liked
    optimistic liked: :liked
  end
end
"#).unwrap();
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(kotlin.contains(r#"${'$'}arg"#), "{kotlin}");
    let unescaped = kotlin.replace("${'$'}", "");
    assert!(!unescaped.contains("$arg"), "{unescaped}");
}

#[test]
fn swift_models_can_be_built_directly_with_defaults_for_sync_state() {
    let models = parse_models("model Moment do\n  attribute :uuid, :string\n  attribute :text, :string, null: true\n  attribute :liked, :bool, default: false\n  attribute :count, :int, default: 3\nend\n").unwrap();
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(swift.contains("public init(id: String, uuid: String, text: String? = nil, liked: Bool = false, count: Int = 3, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:])"), "{swift}");
    assert!(swift.contains("self.liked = liked"), "{swift}");
}

#[test]
fn text_param_relation_takes_a_string_and_keeps_its_name_free() {
    let models = parse_models(r#"model Hit do
  attribute :title, :string
  scope :search do
    parameter :query, :string
    query "HitsSearch"
    response shape: :list
  end
end
"#).unwrap();
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(swift.contains("static func search(_ query: String) throws -> Query<Self>"));
    assert!(swift.contains("params: [\"query\": try JSONValue.encode(query)]"));
    assert!(!swift.contains("var query ="));
    assert!(kotlin.contains("fun search(query: String): Query<Hit>"));
    assert!(!kotlin.contains("var query ="));
}

#[test]
fn action_args_become_parameters_of_the_generated_call() {
    let models = parse_models(r#"model Container do
  attribute :muted, :bool, default: false
  operation :mute do
    mutation "ContainerMute"
    parameter :mute, :bool
    variables containerId: :id, mute: :mute
    optimistic muted: :mute
    wrap "input"
    response from: "containerMute.container"
  end
end
"#).unwrap();
    let action = &models[0].actions[0];
    assert_eq!(action.args["mute"], "bool");
    assert_eq!(action.wrap.as_deref(), Some("input"));
    assert_eq!(action.root.as_deref(), Some("containerMute.container"));
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(swift.contains("@concurrent public func mute(mute: Bool, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self>"));
    assert!(swift.contains("values_[\"muted\"] = try JSONValue.encode(mute)"));
    assert!(swift.contains("args: [\"mute\": try JSONValue.encode(mute)]"));
    assert!(kotlin.contains("suspend fun mute(mute: Boolean, account: AccountScope? = null): MutationReceipt<Container>"));
    assert!(kotlin.contains("\"muted\" to jsonValue(mute)"));
}

#[test]
fn create_takes_only_the_fields_the_mutation_input_maps() {
    let models = parse_models(r#"model Container do
  attribute :name, :string, null: true
  attribute :unreadCount, :int
  attribute :createdAt, :date
  operation :create do
    mutation "ContainerGroupCreate"
    variables name: :name
    local :unreadCount
  end
end
"#).unwrap();
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(swift.contains("@concurrent public static func create(name: String? = nil, unreadCount: Int, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self>"));
    assert!(kotlin.contains("suspend fun create(name: String? = null, unreadCount: Long, account: AccountScope? = null): MutationReceipt<Container>"));
}

#[test]
fn model_listen_lines_become_model_subscriptions() {
    let models = parse_models(r#"model User do
  attribute :online, :bool
  subscribe "userUpdated", debounce: 300.milliseconds
  subscribe "userBlocked", apply: { effect: :delete }
end
"#).unwrap();
    let listens = &models[0].listens;
    assert_eq!(listens.len(), 2);
    assert_eq!((listens[0].subscription.as_str(), listens[0].debounce.as_deref()), ("userUpdated", Some("300.milliseconds")));
    assert!(listens[1].removes);
}

#[test]
fn internal_access_drops_public_and_unmutated_locals_are_constants() {
    let models = parse_models(r#"model Hit do
  attribute :title, :string
  scope :search do
    parameter :query, :string
    query "HitsSearch"
    response shape: :list
  end
end
"#).unwrap();
    let internal = dblayer_gen::emit_swift_with(&models, &[], &dblayer_gen::SchemaTypes::default(), dblayer_gen::Access::Internal);
    assert!(internal.contains("struct Hit: DBModel"));
    assert!(!internal.contains("public "));
    assert!(internal.contains("let built_ = Query<Self>(relation: \"search\""));
}

#[test]
fn model_with_one_single_record_relation_gets_a_singleton_entity() {
    let single = "model Me do\n  attribute :name, :string\n  scope :profile do\n    query \"AccountProfile\"\n  end\nend\n";
    let list = "model Me do\n  attribute :name, :string\n  scope :feed do\n    query \"MesFeed\"\n    response shape: :list\n  end\nend\n";
    let two = "model Me do\n  attribute :name, :string\n  scope :profile do\n    query \"AccountProfile\"\n  end\n  scope :settings do\n    query \"AccountSettings\"\n  end\nend\n";
    for (source, singleton) in [(single, true), (list, false), (two, false)] {
        let models = parse_models(source).unwrap();
        let (swift, kotlin) = (emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default()), emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default()));
        assert_eq!(swift.contains("struct Me: DBSingleton") && swift.contains("static var singleton: Query<Self> { Self.profile }"), singleton, "{swift}");
        assert!(!kotlin.contains("singletonEntityOf"), "{kotlin}");
    }
}

#[test]
fn a_local_field_is_in_the_model_type_but_not_in_create_or_update() {
    let models = parse_models("model Moment do\n  attribute :text, :string\n  attribute :tone, :string, optional: true, local: true\nend\n").unwrap();
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(swift.contains("public let tone: String?") && kotlin.contains("val tone: String? = null"), "{swift}\n{kotlin}");
    assert!(swift.contains("@concurrent public static func create(text: String, in scope_: AccountScope? = nil)") && swift.contains("@concurrent public func update(text: String? = nil, in scope_: AccountScope? = nil)"), "{swift}");
    assert!(kotlin.contains("suspend fun create(text: String, account: AccountScope? = null)") && kotlin.contains("suspend fun update(text: String? = null, account: AccountScope? = null)"), "{kotlin}");
}

#[test]
fn models_and_nested_types_read_positional_cells_on_both_platforms() {
    let models = parse_models("model Moment do\n  attribute :ownerId, :id, null: true\n  attribute :count, :int, default: 2\n  attribute :tags, [:string], default: []\n  attribute :extra, :json, null: true\n  attribute :size, MomentSize, null: true do\n    attribute :w, :int\n  end\n  attribute :boxes, [MomentBox], default: [] do\n    attribute :label, :string\n  end\nend\n").unwrap();
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    for token in [
        "public init(wire row: WireRow) throws",
        "self.ownerId = try r.opt(0, \\.asId)",
        "self.count = try r.req(1, \\.asInt)",
        "self.tags = try r.list(2, \\.asText)",
        "self.extra = try r.jsonOpt(3)",
        "self.size = try r.objOpt(4, MomentSize.init(cells:))",
        "self.boxes = try r.arr(5, MomentBox.init(cells:))",
        "public init(cells: [Cell]) throws",
    ] {
        assert!(swift.contains(token), "missing Swift {token}\n{swift}");
    }
    for token in [
        "companion object : ModelDecoder<Moment>",
        "override fun decode(row: WireRow): Moment",
        "ownerId = r.opt(0, Cell::asId)",
        "count = r.req(1, Cell::asInt)",
        "tags = r.list(2, Cell::asText)",
        "extra = r.jsonOpt(3)",
        "size = r.objOpt(4) { MomentSize.fromCells(it) }",
        "boxes = r.arr(5) { MomentBox.fromCells(it) }",
        "fun fromCells(cells: List<Cell>)",
    ] {
        assert!(kotlin.contains(token), "missing Kotlin {token}\n{kotlin}");
    }
}

#[test]
fn a_local_attribute_with_a_source_is_optional_in_create_and_a_map_reaches_the_call() {
    let models = parse_models(r#"model Container do
  attribute :name, :string
  attribute :unreadCount, :int
  attribute :stance, :string, null: true
  operation :create do
    mutation "ContainerGroupCreate"
    variables name: :name
    local :unreadCount, from: { of: Counter, field: :total }
  end
  operation :move do
    mutation "ContainerMove"
    parameter :shelf, Shelf
    optimistic stance: { from: :shelf, map: { PRIMARY: "main", SECONDARY: "other" } }
  end
end

model Counter do
  attribute :total, :int
  scope :current do
    query "CounterQuery"
  end
end
"#).unwrap();
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(swift.contains("static func create(name: String, unreadCount: Int? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self>"));
    assert!(kotlin.contains("suspend fun create(name: String, unreadCount: Long? = null, account: AccountScope? = null): MutationReceipt<Container>"));
    let map = r#"{"stance":{"$map":{"from":"shelf","map":{"PRIMARY":"main","SECONDARY":"other"}}}}"#;
    assert!(swift.contains(map), "Swift literal");
    assert!(kotlin.contains(&map.replace('$', "${'$'}")), "Kotlin literal");
}

#[test]
fn model_with_two_node_relations_gets_one_entity_per_platform() {
    let source = "model Message do\n  attribute :text, :string\n  scope :node, node: :one do\n    parameter :id, :id\n    query \"MessageNode\"\n    variables id: :id\n  end\n  scope :transcode_status, node: :one do\n    parameter :id, :id\n    query \"MessageTranscodeStatus\"\n    variables id: :id\n  end\nend\n";
    let models = parse_models(source).unwrap();
    let (swift, kotlin) = (emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default()), emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default()));
    assert!(!swift.contains("entity(_ id: String") && !kotlin.contains("fun entity("), "{swift}\n{kotlin}");
    assert!(swift.contains("func node(_ id: String") && swift.contains("func transcodeStatus(_ id: String"), "{swift}");
    assert!(kotlin.contains("fun node(id: String") && kotlin.contains("fun transcodeStatus(id: String"), "{kotlin}");
}

#[test]
fn every_create_form_closes_the_kotlin_companion() {
    let source = |create: &str| format!("model Moment do\n  attribute :text, :string\n{create}\nend\n");
    for create in ["", "  operation :create do\n    mutation \"SendMoment\"\n  end", "  operation :create do\n    mutation \"CreateMoment\"\n  end"] {
        let models = parse_models(&source(create)).unwrap();
        let kotlin = emit_kotlin(&models, &[], &Default::default());
        let opened = kotlin.matches('{').count();
        let closed = kotlin.matches('}').count();
        assert_eq!(opened, closed, "unbalanced braces for create form {create:?}");
    }
}

#[test]
fn every_generated_accessor_that_reaches_the_database_takes_the_account() {
    let models = parse_models(r#"model Moment do
  attribute :text, :string
  scope :feed do
    query "MomentsFeed"
    paginate size: 20, direction: :forward
    edge do
      attribute :impressionUuid, :string, null: true
    end
  end
  scope :vibe_feed do
    parameter :vibeId, :string
    query "MomentsFeed"
    paginate size: 20, direction: :forward
    edge do
      attribute :score, :int, null: true
    end
  end
  operation :like do
    mutation "LikeMoment"
    optimistic false
  end
end

model Me do
  attribute :name, :string
  scope :profile do
    query "AccountProfile"
  end
end
"#).unwrap();
    let (swift, kotlin) = (emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default()), emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default()));
    for line in swift.lines().filter(|line| line.contains("public static func feedEdge(") || line.contains("public static func vibeFeedEdge(")) {
        assert!(line.contains("in scope_: AccountScope? = nil") && line.contains("Self.feedEdge") || line.contains(".edgeValues(of: id, in: scope_)"), "{line}");
    }
    for line in kotlin.lines().filter(|line| line.contains("suspend fun feedEdge(") || line.contains("suspend fun vibeFeedEdge(")) {
        assert!(line.contains("account: AccountScope? = null") && line.contains(".edgeValues(id, account)"), "{line}");
    }
    assert!(kotlin.lines().any(|line| line.contains("suspend fun vibeFeedEdge(vibeId: String, id: String, account: AccountScope? = null)")));
    assert!(swift.contains("vibeFeedEdge(_ vibeId: String, of id: String, in scope_: AccountScope? = nil)"), "{swift}");
    assert!(swift.contains("like(in scope_: AccountScope? = nil)") && kotlin.contains("fun like(account: AccountScope? = null)"));
}

#[test]
fn generated_locals_end_with_an_underscore_and_never_meet_a_dbl_parameter() {
    let models = parse_models(include_str!("../../fixtures/account_names.dbl")).unwrap();
    let (swift, kotlin) = (emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default()), emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default()));
    assert!(swift.contains("entriesEdge(_ scope: String, of id: String, in scope_: AccountScope? = nil)") && swift.contains("Self.entries(scope).edgeValues(of: id, in: scope_)"), "{swift}");
    assert!(swift.contains("func move(scope: String, in scope_: AccountScope? = nil)") && swift.contains("func update(scope: String? = nil, in scope_: AccountScope? = nil)"), "{swift}");
    assert!(kotlin.contains("suspend fun entriesEdge(scope: String, id: String, account: AccountScope? = null)") && kotlin.contains("LedgerFeed.entries(scope).edgeValues(id, account)"), "{kotlin}");
    assert!(kotlin.contains("suspend fun move(scope: String, account: AccountScope? = null)") && kotlin.contains("onAccount(account) { database_, epoch_ ->"), "{kotlin}");
}

#[test]
fn a_query_named_like_a_parameter_is_called_through_its_owner() {
    let models = parse_models(include_str!("../../fixtures/account_names.dbl")).unwrap();
    let (swift, kotlin) = (emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default()), emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default()));
    for call in ["try await Self.scope.edgeValues(of: id, in: scope_)", "try await Self.account.edgeValues(of: id, in: scope_)"] {
        assert!(swift.contains(call), "swift lacks {call}\n{swift}");
    }
    for call in ["Shadow.scope.edgeValues(id, account)", "Shadow.account.edgeValues(id, account)"] {
        assert!(kotlin.contains(call), "kotlin lacks {call}\n{kotlin}");
    }
    assert!(swift.contains("func create(title: String, in scope_: AccountScope? = nil)"), "{swift}");
    assert!(swift.contains("public static var singleton: Query<Self> { Self.account }"), "{swift}");
}

fn refusal_from_schema(source: &str, schema: &str, mutation: &str) -> String {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    std::fs::write(root.join("schema.graphql"), schema).unwrap();
    std::fs::create_dir(root.join("mutations")).unwrap();
    std::fs::write(root.join("mutations/search.graphql"), mutation).unwrap();
    std::fs::write(root.join("search.dbl"), source).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_dblayer-gen")).args(["--schema", "schema.graphql", "swift", "search.dbl"]).current_dir(root).output().unwrap();
    assert!(!output.status.success(), "generation succeeded");
    String::from_utf8_lossy(&output.stderr).to_string()
}

#[test]
fn a_dbl_input_that_would_collide_with_generated_names_is_refused_with_a_diagnostic() {
    let edge = "    query \"Feed\"\n    paginate size: 20, direction: :forward\n    edge do\n      attribute :score, :int, null: true\n    end\n";
    for (case, source, expected) in [
        ("capitalised field", "model Page do\n  attribute :Shadow, :string\nend\n".to_string(), "`Shadow` must start with a lowercase letter"),
        ("capitalised scope parameter", format!("model Page do\n  attribute :title, :string\n  scope :feed do\n    parameter :Shadow, :string\n{edge}  end\nend\n"), "`Shadow` must start with a lowercase letter"),
        ("capitalised action argument", "model Holder do\n  attribute :label, :string\n  operation :set_label do\n    mutation \"SetLabel\"\n    parameter :Holder, :string\n    variables owner: :Holder\n    optimistic false\n  end\nend\n".to_string(), "`Holder` must start with a lowercase letter"),
        ("capitalised operation parameter", "operation :load do\n  query \"Load\"\n  parameter :JSONValue, :string\nend\n".to_string(), "`JSONValue` must start with a lowercase letter"),
        ("trailing underscore field", "model Page do\n  attribute :scope_, :string\nend\n".to_string(), "ends with `_`"),
        ("trailing underscore parameter", format!("model Page do\n  scope :feed do\n    parameter :account_, :string\n{edge}  end\nend\n"), "ends with `_`"),
        ("trailing underscore model", "model Page_ do\n  attribute :title, :string\nend\n".to_string(), "ends with `_`"),
        ("scope parameter id on an edge scope", format!("model Page do\n  attribute :title, :string\n  scope :feed do\n    parameter :id, :id\n    where id: :id\n{edge}  end\nend\n"), "model `Page` scope `feed` edge accessor: parameter `id` has the name of the generated parameter `id`"),
        ("field account", "model Ledger do\n  attribute :account, :string\nend\n".to_string(), "model `Ledger` create: parameter `account` has the name of the generated parameter `account`"),
        ("action argument account", "model Ledger do\n  attribute :title, :string\n  operation :move do\n    mutation \"Move\"\n    parameter :account, :string\n    variables account: :account\n    optimistic false\n  end\nend\n".to_string(), "model `Ledger` action `move`: parameter `account` has the name of the generated parameter `account`"),
        ("scope parameter account", format!("model Page do\n  attribute :title, :string\n  scope :feed do\n    parameter :account, :string\n{edge}  end\nend\n"), "scope `feed` edge accessor: parameter `account` has the name of the generated parameter `account`"),
        ("ephemeral field account", "model Page do\n  attribute :title, :string\n  ephemeral do\n    attribute :account, :string, null: true\n  end\nend\n".to_string(), "model `Page` set: parameter `account` has the name of the generated parameter `account`"),
        ("operation parameter account", "operation :load do\n  query \"Load\"\n  parameter :account, :string\nend\n".to_string(), "operation `load` call: parameter `account` has the name of the generated parameter `account`"),
        ("constructor field uploads", "model Page do\n  attribute :uploads, :string\nend\n".to_string(), "model `Page` constructor: field `uploads` has the name of the generated parameter `uploads`"),
        ("constructor field self", "model Page do\n  attribute :self, :string\nend\n".to_string(), "model `Page` constructor: field `self` has the name of a call or receiver that generated code writes bare"),
        ("operation parameter self", "operation :load do\n  query \"Load\"\n  parameter :self, :string\nend\n".to_string(), "operation `load` call: parameter `self` has the name of a call or receiver that generated code writes bare"),
        ("action argument jsonValue", "model Ledger do\n  attribute :title, :string\n  operation :move do\n    mutation \"Move\"\n    parameter :jsonValue, :string\n    variables value: :jsonValue\n    optimistic false\n  end\nend\n".to_string(), "model `Ledger` action `move`: parameter `jsonValue` has the name of a call or receiver that generated code writes bare"),
        ("model named like an imported type", "model Query do\n  attribute :title, :string\nend\n".to_string(), "type `Query` has the name of a type that generated code imports"),
    ] {
        let errors = parse_models(&source).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(expected)), "{case}: wanted `{expected}`, got {errors:?}");
    }
    for (case, input, schema, expected) in [
        ("input field named like an imported type", "SearchInput", "type Query { a: Int }\ntype Mutation { search(input: SearchInput!): Int }\ninput SearchInput { GraphQLNullable: String!, term: String }\n", "input `SearchInput`: field `GraphQLNullable` has the name of a type that generated code imports"),
        ("input field self", "SearchInput", "type Query { a: Int }\ntype Mutation { search(input: SearchInput!): Int }\ninput SearchInput { self: String! }\n", "input `SearchInput`: field `self` has the name of a call or receiver that generated code writes bare"),
        ("one-of fields with the same case", "SearchInput", "type Query { a: Int }\ntype Mutation { search(input: SearchInput!): Int }\ninput SearchInput @oneOf { value: String, Value: String }\ndirective @oneOf on INPUT_OBJECT\n", "input `SearchInput`: fields `value` and `Value` print the same case `Value`"),
        ("input named like an imported type", "Condition", "type Query { a: Int }\ntype Mutation { search(input: Condition!): Int }\ninput Condition { term: String }\n", "type `Condition` has the name of a type that generated code imports"),
    ] {
        let operation = format!("operation :search do\n  mutation \"Search\"\n  parameter :input, {input}\nend\n");
        let document = format!("mutation Search($input: {input}!) {{ search(input: $input) }}");
        let errors = refusal_from_schema(&operation, schema, &document);
        assert!(errors.contains(expected), "{case}: wanted `{expected}`, got {errors}");
    }
}
