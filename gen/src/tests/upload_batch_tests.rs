use crate::compile_files;

#[test]
fn batch_renumber_requires_known_integer_fields_and_an_explicit_base() {
    let source = &format!("{}{}", crate::TEST_UPLOADS, include_str!("../../../fixtures/asset.dbl"));
    let renumber = |declaration: &str| source.replace("renumber: { position: \"position\", size: \"size\", start: 0 }", declaration);
    for (declaration, accepted) in [
        ("renumber: { position: \"position\", size: \"size\", start: 0 }", true),
        ("renumber: { position: \"position\", size: \"size\", start: 1 }", true),
        ("renumber: { position: \"missing\", size: \"size\", start: 0 }", false),
        ("renumber: { position: \"position\", size: \"missing\", start: 0 }", false),
        ("renumber: { position: \"clientKey\", size: \"size\", start: 0 }", false),
        ("renumber: { position: \"position\", size: \"size\" }", false),
        ("renumber: { position: \"position\", size: \"size\", start: 2 }", false),
        ("renumber: { position: \"position\", size: \"size\", start: 0.5 }", false),
    ] {
        let result = crate::parse_models(&renumber(declaration));
        assert_eq!(result.is_ok(), accepted, "{declaration}: {result:?}");
        if declaration.contains("missing") {
            assert!(result.as_ref().unwrap_err().iter().any(|error| error.message.contains("missing")), "{result:?}");
        }
        if let Ok(models) = result {
            let batch = serde_json::to_value(models[0].actions[1].batch.as_ref().unwrap()).unwrap();
            assert_eq!(batch["renumber"]["position"], "position");
            assert_eq!(batch["renumber"]["size"], "size");
            assert_eq!(batch["renumber"]["position_from"], if declaration.contains("start: 1") { 1 } else { 0 });
        }
    }
    let nested = source.replace("attribute :position, :int, default: 0", "attribute :object, AssetOrder do\n    attribute :position, :int\n    attribute :size, :int\n  end");
    for (position, size, accepted) in [("object.position", "object.size", true), ("object.missing", "object.size", false), ("object.position", "size", true)] {
        let declaration = format!("renumber: {{ position: \"{position}\", size: \"{size}\", start: 1 }}");
        let result = crate::parse_models(&nested.replace("renumber: { position: \"position\", size: \"size\", start: 0 }", &declaration));
        assert_eq!(result.is_ok(), accepted, "{result:?}");
    }
    let unbatched = source.replace("    batch items: \"items\", single: :create_file, input: { clientKey: :clientKey }, renumber: { position: \"position\", size: \"size\", start: 0 }\n", "    renumber position: \"position\", size: \"size\", start: 0\n");
    let errors = crate::parse_models(&unbatched).unwrap_err();
    assert!(errors.iter().any(|error| error.message.contains("renumber")), "{errors:?}");
}

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
        std::fs::write(&path, format!("{}{}", crate::TEST_UPLOADS, include_str!("../../../fixtures/asset.dbl"))).unwrap();
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
    std::fs::write(&path, format!("{}{}", crate::TEST_UPLOADS, r#"model Asset do
  attribute :clientKey, :string
  operation :create_file, upload: true do
    mutation "CreateAsset"
    match_by "clientKey"
    optimistic false
    response from: "createAsset.asset"
  end
  operation :create_files, upload: true do
    mutation "CreateAssets"
    match_by "clientKey"
    optimistic false
    batch items: "items", single: :create_file, input: { clientKey: :clientKey }
    validates :items, length: { minimum: 2, maximum: 10 }
    response from: "createAssets.assets"
  end
end
"#)).unwrap();
    let declarations = compile_files(&[path], None, &[]).unwrap();
    let value = serde_json::to_value(&declarations.models[0]).unwrap();
    let action = value["actions"].as_array().unwrap().iter().find(|action| action["name"] == "create_files").unwrap();
    assert_eq!(action["batch"], serde_json::json!({"min":2,"max":10,"items":"items","single":"create_file","input":{"clientKey":"clientKey"}}));
}
