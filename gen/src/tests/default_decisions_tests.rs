use dblayer::schema::{AssociationKind, Coverage, OnFailure, PageDirection, RemoteShape};

use crate::{compile_files, parse_models, FileError};

pub(super) fn refused(source: &str) -> FileError {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("m.dbl");
    std::fs::write(&file, source).unwrap();
    compile_files(&[file], None, &[]).unwrap_err().remove(0)
}

pub(super) fn refused_at(source: &str, line: usize, word: &str) {
    let error = refused(source);
    assert!(error.file.ends_with("m.dbl"), "{error}");
    assert_eq!(error.error.line, line, "{error}");
    assert!(error.error.message.contains(word), "{error}");
}

const TARGETS: &str = "model Author do\nend\n\nmodel Vibe do\n  attribute :postId, :id\nend\n\nmodel Profile do\n  attribute :postId, :id\nend\n\nmodel Tag do\nend\n\n";
const POST: &str = "model Post do\n  attribute :authorId, :id\n  attribute :tags, [:id], default: []\n  belongs_to :author\n  has_many :vibes\n  has_one :profile\n  references :tags\nend\n";

fn associations() -> Vec<(String, AssociationKind, String, String)> {
    let models = parse_models(&format!("{TARGETS}{POST}")).unwrap();
    let post = models.iter().find(|model| model.key.as_str() == "Post").unwrap();
    post.associations.iter().map(|association| (association.name.clone(), association.kind, association.target.to_string(), association.foreign_key.clone())).collect()
}

#[test]
fn gen_default_page_shape() {
    let shapes: Vec<_> = ["paginate size: 20", "response shape: :list", ""].iter().map(|line| {
        let models = parse_models(&format!("model Sample do\n  scope :feed do\n    query \"Feed\"\n    {line}\n  end\nend\n")).unwrap();
        models[0].relations[0].remote.as_ref().unwrap().shape.clone()
    }).collect();
    assert_eq!(shapes, [
        RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        RemoteShape::List { coverage: Coverage::Complete },
        RemoteShape::Single,
    ]);
}

#[test]
fn gen_default_association_key() {
    let keys: Vec<_> = associations().into_iter().map(|(name, _, _, key)| (name, key)).collect();
    assert_eq!(keys, [("author", "authorId"), ("vibes", "postId"), ("profile", "postId"), ("tags", "tags")].map(|(name, key)| (name.to_string(), key.to_string())));
}

#[test]
fn gen_default_association_target() {
    let targets: Vec<_> = associations().into_iter().map(|(name, kind, target, _)| (name, kind, target)).collect();
    assert_eq!(targets, [
        ("author", AssociationKind::BelongsTo, "Author"), ("vibes", AssociationKind::HasMany, "Vibe"),
        ("profile", AssociationKind::HasOne, "Profile"), ("tags", AssociationKind::References, "Tag"),
    ].map(|(name, kind, target)| (name.to_string(), kind, target.to_string())));
    refused_at("model Post do\n  attribute :ghostId, :id\n  belongs_to :ghost\nend\n", 3, "Ghost");
}

#[test]
fn gen_default_flags() {
    let models = parse_models("model Sample do\n  attribute :name, :string\n  belongs_to :owner, class_name: Sample, foreign_key: :name\n  scope :all do\n    query \"All\"\n  end\nend\n").unwrap();
    let field = models[0].field("name").unwrap();
    assert!(!field.nullable && !field.optional && !field.is_local());
    assert!(!models[0].associations[0].touches_parent);
    assert!(!models[0].relations[0].members_complete);
    refused_at("model Sample do\n  attribute :name, :string, null: \"yes\"\nend\n", 2, "null");
    refused_at("model Sample do\n  attribute :name, :string, optional: 1\nend\n", 2, "optional");
}

#[test]
fn gen_default_action_policy() {
    let models = parse_models("model Sample do\n  operation :create do\n    mutation \"CreateSample\"\n  end\n  operation :poke do\n    mutation \"PokeSample\"\n    optimistic false\n  end\nend\n").unwrap();
    let [create, poke] = &models[0].actions[..] else { panic!("two actions") };
    assert_eq!((create.on_failure, poke.on_failure), (OnFailure::Rollback, OnFailure::Rollback));
    assert!(create.has_optimistic && create.has_temp_row_retention);
    assert_eq!(poke.variables["sampleId"], "id");
}
