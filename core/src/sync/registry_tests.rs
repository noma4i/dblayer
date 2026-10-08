use super::registry::*;

#[test]
fn root_path_is_the_first_field_of_the_operation() {
    let feed = "query feed($vibeId: ID, $after: Int) {\n  feed(vibeId: $vibeId) {\n    edges { node { id } }\n  }\n}";
    assert_eq!(OperationDef::from_document("feed", feed).unwrap().root_path, "feed");
    let aliased = "mutation LikeMoment($momentId: ID!) { like: momentLike(momentId: $momentId) { moment { id } } }";
    assert_eq!(OperationDef::from_document("LikeMoment", aliased).unwrap().root_path, "like");
    let bare = "{ me { id } }";
    assert_eq!(OperationDef::from_document("Me", bare).unwrap().root_path, "me");
    let nested_parens = "query q($a: [Int!]! = [1, 2]) @live { items(ids: [1]) { id } }";
    assert_eq!(OperationDef::from_document("q", nested_parens).unwrap().root_path, "items");
    assert!(OperationDef::from_document("broken", "query broken").is_none());
}
