fn declared(body: &str) -> crate::Declarations {
    crate::parse_all(&[body]).unwrap_or_else(|errors| panic!("{errors:?}"))
}

/// The text of the declaration of `name` in generated code: from its line to the next blank line.
fn declaration(text: &str, name: &str) -> String {
    let start = text.lines().position(|line| (line.contains("struct ") || line.contains("class ")) && line.contains(&format!(" {name}"))).unwrap_or_else(|| panic!("no declaration of {name}"));
    text.lines().skip(start).take_while(|line| !line.is_empty()).collect::<Vec<_>>().join("\n")
}

/// models.md M53, D-287, D-484: an upload action with `returns` and a standalone operation over the same document get the same fields and the same generated types in Swift and Kotlin, named by their owner.
#[test]
fn gen_upload_action_nested_returns() {
    let directory = tempfile::tempdir().unwrap();
    let call = directory.path().join("call.dbl");
    std::fs::write(&call, "operation :photo_call do\n  mutation \"UserPhotoUpdate\"\n  parameter :keepOld, :bool, null: true\n  parameter :skipAssign, :bool, null: true\nend\n").unwrap();
    let mut paths: Vec<std::path::PathBuf> = std::fs::read_dir(crate::package_dir().join("../fixtures")).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "dbl")).collect();
    paths.sort();
    paths.push(call);
    let found = crate::compile_fixtures(&paths).unwrap_or_else(|errors| panic!("{errors:?}"));
    let model = found.models.iter().find(|model| model.key.as_str() == "CurrentUser").expect("the model");
    let returns = model.actions.iter().find(|action| action.name == "update_photo").and_then(|action| action.returns.clone()).expect("the action carries its answer type");
    let result = found.operations.iter().find(|operation| operation.name == "photo_call").and_then(|operation| operation.result.clone()).expect("the standalone answer type");
    let renamed = |text: String| text.replace("CurrentUserUpdatePhotoResult", "PhotoCallResult");
    assert_eq!(renamed(serde_json::to_string(&returns.fields).unwrap()), serde_json::to_string(&result.fields).unwrap(), "one inference: the fields of the action are the fields of the standalone operation");
    assert!(returns.fields.iter().all(|field| field.nullable), "every answer field is nullable");
    let (swift, kotlin) = (crate::emit_swift(&found.models, &found.operations, &found.inputs), crate::emit_kotlin(&found.models, &found.operations, &found.inputs));
    for type_name in ["UserPhoto", "UserPhotoMedia"] {
        let (action_type, call_type) = (format!("CurrentUserUpdatePhotoResult{type_name}"), format!("PhotoCallResult{type_name}"));
        assert_eq!(renamed(declaration(&swift, &action_type)), declaration(&swift, &call_type), "swift type {type_name}");
        assert_eq!(renamed(declaration(&kotlin, &action_type)), declaration(&kotlin, &call_type), "kotlin type {type_name}");
    }
    assert!(declaration(&swift, "CurrentUserUpdatePhotoResultUserPhotoMedia").contains("thumbUrl: String?"));
    assert!(declaration(&kotlin, "CurrentUserUpdatePhotoResultUserPhotoMedia").contains("val thumbUrl: String? = null"));
}

fn fixtures() -> crate::Declarations {
    let mut models: Vec<std::path::PathBuf> = std::fs::read_dir(crate::package_dir().join("../fixtures")).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "dbl")).collect();
    models.sort();
    crate::compile_fixtures(&models).unwrap_or_else(|errors| panic!("{errors:?}"))
}

/// D-287: an upload action has a call on the type of the model, so an upload needs no instance: a singleton takes no id, a model takes its id; the instance method calls the one on the type, with the same variables on both platforms.
#[test]
fn gen_upload_action_type_call() {
    let found = fixtures();
    let (swift, kotlin) = (crate::emit_swift(&found.models, &found.operations, &found.inputs), crate::emit_kotlin(&found.models, &found.operations, &found.inputs));
    let swift_type = |name: &str| { let start = swift.find(&format!("public struct {name}:")).expect("the model"); &swift[start..start + swift[start..].find("\npublic struct ").unwrap_or(swift.len() - start)] };
    let current = swift_type("CurrentUser");
    assert!(current.contains("public static func updatePhoto(source: String, keepOld: Bool? = nil, skipAssign: Bool? = nil, prepare: Data? = nil, maxBytes: UInt64? = nil, in scope_: AccountScope) async throws -> AnsweredReceipt<Self, CurrentUserUpdatePhotoResult>"), "singleton: no id");
    assert!(current.contains("scope_.upload(Self.self, id: \"singleton\", action: \"update_photo\", UploadRequest(path: source, variables: variables_, prepare: prepare, maxBytes: maxBytes)).answered(CurrentUserUpdatePhotoResult.self)"));
    assert!(current.contains("public func updatePhoto(_ request: UploadRequest, in scope_: AccountScope) async throws -> AnsweredReceipt<Self, CurrentUserUpdatePhotoResult> {\n        try await scope_.upload(Self.self, id: id, action: \"update_photo\", request).answered(CurrentUserUpdatePhotoResult.self)"), "the instance method calls the one on the type");
    let container = swift_type("Container");
    assert!(container.contains("public static func updatePhoto(id: String, source: String") && container.contains("scope_.upload(Self.self, id: id, action: \"update_photo\""), "a model takes its id");
    assert!(kotlin.contains("suspend fun updatePhoto(account: AccountScope, source: String, keepOld: Boolean? = null, skipAssign: Boolean? = null, prepare: ByteArray? = null, maxBytes: ULong? = null): AnsweredReceipt<CurrentUser, CurrentUserUpdatePhotoResult> =\n            account.upload(this, \"singleton\", \"update_photo\", UploadRequest(source, variables = buildJsonObject { if (keepOld != null) this.put(\"keepOld\", jsonValue(keepOld)); if (skipAssign != null) this.put(\"skipAssign\", jsonValue(skipAssign)) }, prepare = prepare, maxBytes = maxBytes)).answered(CurrentUserUpdatePhotoResult.serializer())"), "kotlin singleton");
    assert!(kotlin.contains("suspend fun updatePhoto(account: AccountScope, id: String, source: String"), "kotlin model takes its id");
    assert!(kotlin.contains("suspend fun updatePhoto(account: AccountScope, request: UploadRequest): AnsweredReceipt<CurrentUser, CurrentUserUpdatePhotoResult> = account.upload(CurrentUser, id, \"update_photo\", request).answered(CurrentUserUpdatePhotoResult.serializer())"), "kotlin instance method calls the one on the type");
}

/// A generated Swift binding that is never assigned is declared with `let`: generated code builds with warnings as errors.
#[test]
fn gen_swift_variables_binding_is_mutated() {
    let found = fixtures();
    let swift = crate::emit_swift(&found.models, &found.operations, &found.inputs);
    let lines: Vec<&str> = swift.lines().collect();
    let never: Vec<usize> = lines.iter().enumerate().filter(|(_, line)| line.trim_start().starts_with("var variables_")).filter(|(at, _)| !lines[at + 1..].iter().take_while(|line| !line.contains("return ")).any(|line| line.contains("variables_[\""))).map(|(at, _)| at + 1).collect();
    assert!(never.is_empty(), "`var variables_` is never assigned at generated lines {never:?}");
}

/// D-480: a non-upload action of a singleton model has a call on the type that needs no row, on both platforms; a model that is no singleton gets none.
#[test]
fn gen_singleton_action_type_call() {
    let source = "model Viewer do\n  attribute :id, :id\n  attribute :registered, :bool, null: true\n  scope :current do\n    query \"ViewerQuery\"\n  end\n  operation :complete do\n    mutation \"ViewerComplete\"\n    optimistic false\n    response from: \"viewerComplete.viewer\"\n  end\nend\nmodel Item do\n  attribute :id, :id\n  attribute :done, :bool, null: true\n  operation :finish do\n    mutation \"ItemFinish\"\n    optimistic false\n    response from: \"itemFinish.item\"\n  end\nend\n";
    let found = declared(source);
    let (swift, kotlin) = (crate::emit_swift(&found.models, &found.operations, &found.inputs), crate::emit_kotlin(&found.models, &found.operations, &found.inputs));
    let swift_type = |name: &str| { let start = swift.find(&format!("public struct {name}:")).expect("the model"); &swift[start..start + swift[start..].find("\npublic struct ").unwrap_or(swift.len() - start)] };
    let viewer = swift_type("Viewer");
    assert!(viewer.contains("public static func complete(") && viewer.contains("actOnSingleton(\"complete\""), "swift singleton call on the type: {viewer}");
    assert!(!swift_type("Item").contains("actOnSingleton"), "a model that is no singleton has no call on the type");
    assert!(kotlin.contains("this.actOnSingleton(\"complete\""), "kotlin singleton call on the type");
    assert_eq!(kotlin.matches("actOnSingleton(").count(), 1, "kotlin: only the singleton action is called on the type");
}
