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
    assert!(swift.contains("@concurrent public func mute(mute: Bool, in scope: AccountScope? = nil) async throws -> Self"));
    assert!(swift.contains("values[\"muted\"] = try JSONValue.encode(mute)"));
    assert!(swift.contains("args: [\"mute\": try JSONValue.encode(mute)]"));
    assert!(kotlin.contains("suspend fun mute(mute: Boolean, account: AccountScope? = null): Container"));
    assert!(kotlin.contains("\"muted\" to jsonValue(mute)"));
}

#[test]
fn create_takes_only_the_fields_the_mutation_input_maps() {
    let models = parse_models(r#"model Container do
  attribute :name, :string, null: true
  attribute :unreadCount, :int
  attribute :createdAt, :date
  operation :create, durable: true do
    mutation "ContainerGroupCreate"
    variables name: :name
    local :unreadCount
  end
end
"#).unwrap();
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(swift.contains("@concurrent public static func send(in scope: AccountScope, name: String? = nil, unreadCount: Int) async throws -> Sent<Self>"));
    assert!(kotlin.contains("suspend fun send(account: AccountScope, name: String? = null, unreadCount: Long): Sent<Container>"));
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
    assert!(internal.contains("let built = Query<Self>(relation: \"search\""));
}

#[test]
fn model_with_one_single_record_relation_gets_a_singleton_entity() {
    let single = "model Me do\n  attribute :name, :string\n  scope :profile do\n    query \"AccountProfile\"\n  end\nend\n";
    let list = "model Me do\n  attribute :name, :string\n  scope :feed do\n    query \"MesFeed\"\n    response shape: :list\n  end\nend\n";
    let two = "model Me do\n  attribute :name, :string\n  scope :profile do\n    query \"AccountProfile\"\n  end\n  scope :settings do\n    query \"AccountSettings\"\n  end\nend\n";
    for (source, singleton) in [(single, true), (list, false), (two, false)] {
        let models = parse_models(source).unwrap();
        let (swift, kotlin) = (emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default()), emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default()));
        assert_eq!(swift.contains("struct Me: DBSingleton") && swift.contains("static var singleton: Query<Self> { profile }"), singleton, "{swift}");
        assert!(!kotlin.contains("singletonEntityOf"), "{kotlin}");
    }
}

#[test]
fn a_local_field_is_in_the_model_type_but_not_in_create_or_update() {
    let models = parse_models("model Moment do\n  attribute :text, :string\n  attribute :tone, :string, optional: true, local: true\nend\n").unwrap();
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(swift.contains("public let tone: String?") && kotlin.contains("val tone: String? = null"), "{swift}\n{kotlin}");
    assert!(swift.contains("@concurrent public static func create(text: String, in scope: AccountScope? = nil)") && swift.contains("@concurrent public func update(text: String? = nil, in scope: AccountScope? = nil)"), "{swift}");
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
  operation :create, durable: true do
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
    assert!(swift.contains("static func send(in scope: AccountScope, name: String, unreadCount: Int? = nil) async throws -> Sent<Self>"));
    assert!(kotlin.contains("suspend fun send(account: AccountScope, name: String, unreadCount: Long? = null): Sent<Container>"));
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
    for create in ["", "  operation :create, durable: true do\n    mutation \"SendMoment\"\n  end", "  operation :create do\n    mutation \"CreateMoment\"\n  end"] {
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
  operation :set_avatar do
    mutation "SetAvatar"
    optimistic false
    parameter :photo, :blob
    variables photo: :photo
  end
end
"#).unwrap();
    let (swift, kotlin) = (emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default()), emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default()));
    for line in swift.lines().filter(|line| line.contains("public static func feedEdge(") || line.contains("public static func vibeFeedEdge(")) {
        assert!(line.contains("in scope: AccountScope? = nil") && line.contains("edgeValues(of: id, in: scope)"), "{line}");
    }
    for line in kotlin.lines().filter(|line| line.contains("suspend fun feedEdge(") || line.contains("suspend fun vibeFeedEdge(")) {
        assert!(line.contains("account: AccountScope? = null") && line.contains("edgeValues(id, account)"), "{line}");
    }
    assert!(kotlin.lines().any(|line| line.contains("suspend fun vibeFeedEdge(vibeId: String, id: String, account: AccountScope? = null)")));
    assert!(swift.contains("vibeFeedEdge(_ vibeId: String, of id: String, in scope: AccountScope? = nil)"), "{swift}");
    assert!(kotlin.lines().any(|line| line.contains("suspend fun setAvatar(photo: Blob, account: AccountScope? = null)") && line.contains("profile.read(account)") && line.contains(".setAvatar(photo, account)")), "{kotlin}");
    assert!(swift.contains("like(in scope: AccountScope? = nil)") && kotlin.contains("fun like(account: AccountScope? = null)"));
}

#[test]
fn the_account_parameter_never_takes_the_name_of_a_dbl_parameter() {
    let models = parse_models(include_str!("../../fixtures/account_names.dbl")).unwrap();
    let (swift, kotlin) = (emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default()), emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default()));
    assert!(swift.contains("entriesEdge(_ scope: String, _ account: String, of id: String, in scope_: AccountScope? = nil)") && swift.contains("edgeValues(of: id, in: scope_)"), "{swift}");
    assert!(swift.contains("func move(account: String, scope: String, in scope_: AccountScope? = nil)") && swift.contains("func update(scope: String? = nil, account: String?? = nil, in scope_: AccountScope? = nil)"), "{swift}");
    assert!(kotlin.contains("suspend fun entriesEdge(scope: String, account: String, id: String, account_: AccountScope? = null)") && kotlin.contains("edgeValues(id, account_)"), "{kotlin}");
    assert!(kotlin.contains("suspend fun move(account: String, scope: String, account_: AccountScope? = null)") && kotlin.contains("onAccount(account_)"), "{kotlin}");
}

#[test]
fn the_account_parameter_never_takes_the_name_of_the_called_query() {
    let models = parse_models(include_str!("../../fixtures/account_names.dbl")).unwrap();
    let (swift, kotlin) = (emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default()), emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default()));
    assert!(swift.contains("scopeEdge(of id: String, in scope_: AccountScope? = nil)") && swift.contains("try await scope.edgeValues(of: id, in: scope_)"), "{swift}");
    assert!(swift.contains("func create(title: String, in scope_: AccountScope? = nil)"), "{swift}");
    assert!(kotlin.contains("suspend fun accountEdge(id: String, account_: AccountScope? = null)") && kotlin.contains("account.edgeValues(id, account_)"), "{kotlin}");
    assert!(kotlin.contains("onAccount(account_)"), "{kotlin}");
}
