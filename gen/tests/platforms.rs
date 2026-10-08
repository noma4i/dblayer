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
    for token in ["struct Moment", "DBModel", "func create(", "func set(", "static var recent", "static var feed"] {
        assert!(swift.contains(token), "missing Swift {token}");
    }
    for token in ["@Serializable", "data class Moment", "fun create(", "fun set(", "val recent", "val feed"] {
        assert!(kotlin.contains(token), "missing Kotlin {token}");
    }
}
