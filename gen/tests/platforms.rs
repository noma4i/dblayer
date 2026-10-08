use dblayer_gen::{parse_models, emit_swift, emit_kotlin};

#[test]
fn platform_models_offer_typed_writes_and_live_scopes() {
    let models = parse_models(r#"
model Moment do
  schema do
    string :text
    id :userId, null: true
    date :createdAt
    ephemeral do
      bool :selected, default: false
    end
  end
  belongs_to :user
  scope :recent, order: { createdAt: :desc }
  action :like, mutation: LikeMoment, optimistic: { text: "liked" }
  remote :feed do
    list
  end
end
model User do
  schema do
    string :name
  end
end
"#).unwrap();
    let moment = &models[0];
    assert_eq!(moment.associations[0].target.as_str(), "User");
    assert_eq!(moment.associations[0].foreign_key, "userId");
    assert!(moment.is_indexed("userId") && moment.is_indexed("createdAt"));
    assert_eq!(moment.actions[0].variables["momentId"], "id");
    assert_eq!(moment.relations[1].remote.as_ref().unwrap().operation, "MomentsFeed");
    let swift = emit_swift(&models);
    let kotlin = emit_kotlin(&models);
    for token in ["struct Moment", "DBModel", "func create(", "func set(", "static var recent", "static var feed", "func like("] {
        assert!(swift.contains(token), "missing Swift {token}");
    }
    for token in ["@Serializable", "data class Moment", "fun create(", "fun set(", "val recent", "val feed", "fun like(", "fun ids("] {
        assert!(kotlin.contains(token), "missing Kotlin {token}");
    }
}

#[test]
fn object_field_can_name_the_app_type_it_decodes_to() {
    let models = parse_models("model Moment do\n  schema do\n    object :media, as: MomentMedia, null: true\n    array :widgets, as: \"[MomentWidget]\"\n  end\nend\n").unwrap();
    let swift = emit_swift(&models);
    assert!(swift.contains("public let media: MomentMedia?"), "{swift}");
    assert!(swift.contains("public let widgets: [MomentWidget]"), "{swift}");
    assert!(swift.contains("Column<Moment, MomentMedia?>(\"media\")"), "{swift}");
}

#[test]
fn swift_models_can_be_built_directly_with_defaults_for_sync_state() {
    let models = parse_models("model Moment do\n  schema do\n    string :uuid\n    string :text, null: true\n    bool :liked, default: false\n    int :count, default: 3\n  end\nend\n").unwrap();
    let swift = emit_swift(&models);
    assert!(swift.contains("public init(id: String, uuid: String, text: String? = nil, liked: Bool = false, count: Int = 3, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil)"), "{swift}");
    assert!(swift.contains("self.liked = liked"), "{swift}");
}

#[test]
fn text_param_relation_takes_a_string_and_keeps_its_name_free() {
    let models = parse_models(r#"
model Hit do
  schema do
    string :title
  end
  remote :search, text: [:query] do
    list
  end
end
"#).unwrap();
    let swift = emit_swift(&models);
    let kotlin = emit_kotlin(&models);
    assert!(swift.contains("static func search(_ query: String) -> Query<Self>"));
    assert!(swift.contains("params: [\"query\": JSONValue.encode(query)]"));
    assert!(!swift.contains("var query ="));
    assert!(kotlin.contains("fun search(query: String): Query<Hit>"));
    assert!(!kotlin.contains("var query ="));
}

#[test]
fn action_args_become_parameters_of_the_generated_call() {
    let models = parse_models(r#"
model Chat do
  schema do
    bool :muted, default: false
  end
  action :mute do
    mutation ChatMute
    args       mute: :bool
    input      chatId: :id, mute: :mute
    optimistic muted: :mute
    wrap       :input
    root       "chatMute.chat"
  end
end
"#).unwrap();
    let action = &models[0].actions[0];
    assert_eq!(action.args["mute"], "bool");
    assert_eq!(action.wrap.as_deref(), Some("input"));
    assert_eq!(action.root.as_deref(), Some("chatMute.chat"));
    let swift = emit_swift(&models);
    let kotlin = emit_kotlin(&models);
    assert!(swift.contains("public func mute(mute: Bool) async throws -> Self"));
    assert!(swift.contains("values[\"muted\"] = JSONValue.encode(mute)"));
    assert!(swift.contains("args: [\"mute\": JSONValue.encode(mute)]"));
    assert!(kotlin.contains("suspend fun mute(mute: Boolean): Chat"));
    assert!(kotlin.contains("\"muted\" to jsonValue(mute)"));
}

#[test]
fn create_takes_only_the_fields_the_mutation_input_maps() {
    let models = parse_models(r#"
model Chat do
  schema do
    string :name, null: true
    int    :unreadCount
    date   :createdAt
  end
  create with: ChatGroupCreate, input: { name: :name }, local: [:unreadCount]
end
"#).unwrap();
    let swift = emit_swift(&models);
    let kotlin = emit_kotlin(&models);
    assert!(swift.contains("public static func create(name: String? = nil, unreadCount: Int) async throws -> Self"));
    assert!(kotlin.contains("suspend fun create(name: String? = null, unreadCount: Long): Chat"));
}
