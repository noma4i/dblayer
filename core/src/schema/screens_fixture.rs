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
        "SampleAction",
        vec![
            FieldSpec::variant("navigate", "SampleNavigateAction", vec![FieldSpec::scalar("slug", ScalarKind::Str), FieldSpec::scalar("title", ScalarKind::Str).nullable()]),
            FieldSpec::variant("openRecord", "SampleOpenRecordAction", vec![FieldSpec::scalar("recordId", ScalarKind::Str)]),
        ],
    )
    .nullable()
}

fn text_value(name: &str) -> FieldSpec {
    FieldSpec::union(
        name,
        "SampleTextValue",
        vec![
            FieldSpec::variant("literal", "SampleTextLiteral", vec![FieldSpec::scalar("text", ScalarKind::Str)]),
            FieldSpec::variant("stateRef", "SampleStateRef", vec![FieldSpec::scalar("path", ScalarKind::Str)]),
        ],
    )
    .nullable()
}

pub(crate) fn elements(variants: Vec<FieldSpec>) -> FieldSpec {
    let shared = vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::list("children", ScalarKind::Id), action()];
    FieldSpec::union_array("elements", "SampleElement", variants).with_shared(shared)
}

pub(crate) fn screen_variants() -> Vec<FieldSpec> {
    vec![
        FieldSpec::variant("button", "SampleButton", vec![text_value("label"), FieldSpec::scalar("disabled", ScalarKind::Bool).nullable()]),
        FieldSpec::variant("heading", "SampleHeading", vec![FieldSpec::scalar("text", ScalarKind::Str), FieldSpec::list("tags", ScalarKind::Str)]),
        FieldSpec::variant("column", "SampleColumn", vec![FieldSpec::scalar("gap", ScalarKind::Num).nullable()]),
        FieldSpec::variant("pressable", "SamplePressable", vec![]),
    ]
}

pub(crate) fn screens_spec() -> ModelSpec {
    let spec = typed(FieldSpec::object("spec"), "SampleSpec", vec![FieldSpec::scalar("root", ScalarKind::Id), elements(screen_variants())]).nullable();
    let mut model = ModelSpec::new("screens", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("slug", ScalarKind::Str).optional(), spec]);
    resolve_types(std::slice::from_mut(&mut model));
    model
}
