use crate::compile_files;

#[test]
fn batch_items_require_an_input_list_with_the_key_and_signed_id() {
    for (items, fields, single_key, accepted) in [
        ("[AssetInput!]!", "clientKey: String!, signedId: String!", true, true),
        ("AssetInput!", "clientKey: String!, signedId: String!", true, false),
        ("[AssetInput!]!", "clientKey: String!", true, false),
        ("[AssetInput!]!", "signedId: String!", true, false),
        ("[AssetInput!]!", "clientKey: String!, signedId: String!", false, false),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("asset.dbl");
        std::fs::write(&path, include_str!("../../fixtures/asset.dbl")).unwrap();
        let schema = directory.path().join("schema.graphql");
        std::fs::write(&schema, format!("type Query {{ ping: Int }} type Mutation {{ createAssets(items: {items}): AssetsPayload, createAsset(signedId: String!, clientKey: String): AssetPayload }} input AssetInput {{ {fields} }} type AssetsPayload {{ assets: [Asset!]! }} type AssetPayload {{ asset: Asset! }} type Asset {{ id: ID!, clientKey: String!, size: Int! }}")).unwrap();
        let document = directory.path().join("mutations.graphql");
        let key_variable = if single_key { ", $clientKey: String!" } else { "" };
        let key_argument = if single_key { ", clientKey: $clientKey" } else { "" };
        std::fs::write(&document, format!("mutation CreateAssets($items: {items}) {{ createAssets(items: $items) {{ assets {{ id clientKey size }} }} }} mutation CreateAsset($signedId: String!{key_variable}) {{ createAsset(signedId: $signedId{key_argument}) {{ asset {{ id clientKey size }} }} }}")).unwrap();
        let result = compile_files(&[path], Some(&schema), &[document]);
        assert_eq!(result.is_ok(), accepted, "items={items}, fields={fields}: {result:?}");
    }
}

#[test]
fn upload_batch_declaration_reaches_the_core_spec() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("asset.dbl");
    std::fs::write(&path, r#"
model Asset do
  field :clientKey, type: :string
  operation :create_file do
    upload CreateAsset
    correlate :clientKey
    root "createAsset.asset"
    local_effect :none, reason: "created with its file"
  end
  operation :create_files do
    upload CreateAssets
    batch min: 2, max: 10, items: :items, single: :create_file
    item clientKey: :clientKey
    correlate :clientKey
    root "createAssets.assets"
    local_effect :none, reason: "created with their files"
  end
end
"#).unwrap();
    let declarations = compile_files(&[path], None, &[]).unwrap();
    let value = serde_json::to_value(&declarations.models[0]).unwrap();
    let action = value["actions"].as_array().unwrap().iter().find(|action| action["name"] == "create_files").unwrap();
    assert_eq!(action["batch"], serde_json::json!({"min":2,"max":10,"items":"items","single":"create_file","input":{"clientKey":"clientKey"}}));
}
