use dblayer::schema::AssociationKind;

use crate::check::check_source;

#[test]
fn inv_dsl_05_an_association_derives_its_model_and_key_from_its_name_and_a_foreign_key_needs_no_index_line() {
    let models = check_source("model User do\n  has_many :moments\nend\n\nmodel Moment do\n  attribute :userId, :id\n  belongs_to :user\nend\n").unwrap();
    let owner = &models[0].associations[0];
    assert_eq!((owner.kind, owner.target.as_str(), owner.foreign_key.as_str()), (AssociationKind::HasMany, "Moment", "userId"));
    let moment = &models[1];
    let parent = &moment.associations[0];
    assert_eq!((parent.kind, parent.target.as_str(), parent.foreign_key.as_str()), (AssociationKind::BelongsTo, "User", "userId"));
    assert!(moment.indexes.contains(&vec!["userId".to_string()]) || moment.fields.iter().any(|field| field.name == "userId" && field.indexed));
}
