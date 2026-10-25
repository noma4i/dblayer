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
    assert!(swift.contains("public init(id: String, uuid: String, text: String? = nil, liked: Bool = false, count: Int = 3, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil)"), "{swift}");
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
    assert!(swift.contains("public func mute(mute: Bool) async throws -> Self"));
    assert!(swift.contains("values[\"muted\"] = try JSONValue.encode(mute)"));
    assert!(swift.contains("args: [\"mute\": try JSONValue.encode(mute)]"));
    assert!(kotlin.contains("suspend fun mute(mute: Boolean): Container"));
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
    assert!(swift.contains("public static func create(name: String? = nil, unreadCount: Int) async throws -> Self"));
    assert!(kotlin.contains("suspend fun create(name: String? = null, unreadCount: Long): Container"));
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
        assert_eq!(kotlin.contains("fun entity() = singletonEntityOf(\"Me\", this, profile)"), singleton, "{kotlin}");
    }
}

#[test]
fn a_local_field_is_in_the_model_type_but_not_in_create_or_update() {
    let models = parse_models("model Moment do\n  attribute :text, :string\n  attribute :tone, :string, optional: true, local: true\nend\n").unwrap();
    let swift = emit_swift(&models, &[], &dblayer_gen::SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &dblayer_gen::SchemaTypes::default());
    assert!(swift.contains("public let tone: String?") && kotlin.contains("val tone: String? = null"), "{swift}\n{kotlin}");
    assert!(swift.contains("public static func create(text: String)") && swift.contains("public func update(text: String? = nil)"), "{swift}");
    assert!(kotlin.contains("suspend fun create(text: String)") && kotlin.contains("suspend fun update(text: String? = null)"), "{kotlin}");
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
