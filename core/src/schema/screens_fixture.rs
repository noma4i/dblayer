use super::{resolve_types, FieldSpec, ModelSpec};
use crate::value::ScalarKind;

fn typed(mut field: FieldSpec, name: &str, children: Vec<FieldSpec>) -> FieldSpec {
    field.app_type = Some(name.to_string());
    field.fields = children;
    field
}

pub(crate) fn action() -> FieldSpec {
    FieldSpec::union(
        "press",
        "ScreenAction",
        vec![
            FieldSpec::variant("navigate", "ScreenNavigateAction", vec![FieldSpec::scalar("slug", ScalarKind::Str), FieldSpec::scalar("title", ScalarKind::Str).nullable()]),
            FieldSpec::variant("openChat", "ScreenOpenChatAction", vec![FieldSpec::scalar("chatId", ScalarKind::Str)]),
        ],
    )
    .nullable()
}

fn text_value(name: &str) -> FieldSpec {
    FieldSpec::union(
        name,
        "ScreenTextValue",
        vec![
            FieldSpec::variant("literal", "ScreenTextLiteral", vec![FieldSpec::scalar("text", ScalarKind::Str)]),
            FieldSpec::variant("stateRef", "ScreenStateRef", vec![FieldSpec::scalar("path", ScalarKind::Str)]),
        ],
    )
    .nullable()
}

pub(crate) fn elements(variants: Vec<FieldSpec>) -> FieldSpec {
    let shared = vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::list("children", ScalarKind::Id), action()];
    FieldSpec::union_array("elements", "ScreenElement", variants).with_shared(shared)
}

pub(crate) fn screen_variants() -> Vec<FieldSpec> {
    vec![
        FieldSpec::variant("button", "ScreenButton", vec![text_value("label"), FieldSpec::scalar("disabled", ScalarKind::Bool).nullable()]),
        FieldSpec::variant("heading", "ScreenHeading", vec![FieldSpec::scalar("text", ScalarKind::Str), FieldSpec::list("tags", ScalarKind::Str)]),
        FieldSpec::variant("column", "ScreenColumn", vec![FieldSpec::scalar("gap", ScalarKind::Num).nullable()]),
        FieldSpec::variant("pressable", "ScreenPressable", vec![]),
    ]
}

pub(crate) fn screens_spec() -> ModelSpec {
    let spec = typed(FieldSpec::object("spec"), "ScreenSpec", vec![FieldSpec::scalar("root", ScalarKind::Id), elements(screen_variants())]).nullable();
    let mut model = ModelSpec::new("screens", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("slug", ScalarKind::Str).optional(), spec]);
    resolve_types(std::slice::from_mut(&mut model));
    model
}
