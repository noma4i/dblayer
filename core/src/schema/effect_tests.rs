use serde_json::json;

use super::{validate_models, ActionEffect, DeclarationError, EffectFault, EffectSource, ModelSpec, Selection};
use crate::db::spec_fixtures::specs;

fn fault_of(change: impl Fn(&mut Vec<ModelSpec>)) -> Option<EffectFault> {
    let mut models = specs();
    change(&mut models);
    validate_models(&models, &[]).into_iter().find_map(|fault| match fault.error {
        DeclarationError::Effect { fault, .. } => Some(fault),
        _ => None,
    })
}

fn effects_of<'a>(models: &'a mut [ModelSpec], model: &str, action: &str) -> &'a mut Vec<ActionEffect> {
    let owner = models.iter_mut().find(|spec| spec.key.as_str() == model).unwrap();
    &mut owner.actions.iter_mut().find(|declared| declared.name == action).unwrap().effects
}

fn selection(model: &str, scope: &str) -> Selection {
    Selection { model: model.to_string(), scope: scope.to_string(), rows: None, with: Default::default() }
}

#[test]
fn the_fixture_effects_are_valid() {
    assert_eq!(fault_of(|_| {}), None);
}

#[test]
fn an_effect_the_models_cannot_carry_out_is_a_declaration_error() {
    assert_eq!(fault_of(|models| effects_of(models, "people", "block")[0] = ActionEffect::Add(selection("ghosts", "blocked"))), Some(EffectFault::UnknownModel("ghosts".into())));
    assert_eq!(fault_of(|models| effects_of(models, "people", "block")[0] = ActionEffect::Add(selection("people", "ghost"))), Some(EffectFault::UnknownScope { model: "people".into(), scope: "ghost".into() }));
    assert_eq!(fault_of(|models| effects_of(models, "people", "block")[0] = ActionEffect::Add(selection("rooms", "open"))), Some(EffectFault::RowsModel("open".into())));
    assert_eq!(
        fault_of(|models| effects_of(models, "rooms", "add_members")[0] = ActionEffect::Add(Selection { rows: Some("ghost".into()), ..selection("people", "roster") })),
        Some(EffectFault::RowsParameter("ghost".into()))
    );
    assert_eq!(
        fault_of(|models| effects_of(models, "rooms", "add_members")[0] = ActionEffect::Add(Selection { rows: Some("personIds".into()), ..selection("people", "roster") })),
        Some(EffectFault::UnboundParameter { scope: "roster".into(), parameter: "roomId".into() })
    );
    assert_eq!(
        fault_of(|models| effects_of(models, "rooms", "remove_members")[0] = ActionEffect::Remove(Selection { rows: Some("personIds".into()), with: [("roomId".to_string(), EffectSource::Owner("id".into()))].into(), ..selection("people", "roster") })),
        Some(EffectFault::PartialBinding("roster".into()))
    );
    assert_eq!(
        fault_of(|models| effects_of(models, "rooms", "add_members")[0] = ActionEffect::Add(Selection { rows: Some("personIds".into()), with: [("ghost".to_string(), EffectSource::Owner("id".into()))].into(), ..selection("people", "roster") })),
        Some(EffectFault::ScopeParameter { scope: "roster".into(), parameter: "ghost".into() })
    );
}

#[test]
fn a_created_or_changed_row_must_name_declared_models_fields_and_parameters() {
    let create = |values: &[(&str, EffectSource)]| ActionEffect::Create { model: "notes".into(), values: values.iter().map(|(name, source)| (name.to_string(), source.clone())).collect() };
    let change = |model: &str, row: Option<&str>, values: &[(&str, EffectSource)]| ActionEffect::Change { model: model.into(), row: row.map(str::to_string), values: values.iter().map(|(name, source)| (name.to_string(), source.clone())).collect() };
    assert_eq!(fault_of(|models| effects_of(models, "people", "send_gift")[0] = create(&[("ghost", EffectSource::Literal(json!(1)))])), Some(EffectFault::UnknownField { model: "notes".into(), field: "ghost".into() }));
    assert_eq!(fault_of(|models| effects_of(models, "people", "send_gift")[0] = create(&[("body", EffectSource::Arg("ghost".into()))])), Some(EffectFault::UnknownArg("ghost".into())));
    assert_eq!(fault_of(|models| effects_of(models, "people", "send_gift")[0] = create(&[("body", EffectSource::Plus("price".into()))])), Some(EffectFault::PlusOutsideChange));
    assert_eq!(fault_of(|models| effects_of(models, "people", "send_gift").push(create(&[]))), Some(EffectFault::CreatedTwice("notes".into())));
    assert_eq!(fault_of(|models| effects_of(models, "people", "send_gift")[1] = change("notes", None, &[("body", EffectSource::Literal(json!("x")))])), Some(EffectFault::NotSingleton("notes".into())));
    assert_eq!(fault_of(|models| effects_of(models, "people", "send_gift")[1] = change("wallets", None, &[("balance", EffectSource::Minus("giftId".into()))])), Some(EffectFault::NotNumeric("balance".into())));
    assert_eq!(fault_of(|models| effects_of(models, "people", "send_gift")[1] = change("wallets", Some("price"), &[("balance", EffectSource::Minus("price".into()))])), Some(EffectFault::RowsParameter("price".into())));
}
