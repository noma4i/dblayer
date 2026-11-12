use crate::test_wait::limited;

const LIMIT: std::time::Duration = std::time::Duration::from_secs(20);
use super::lifecycle::explore_inputs;
use super::store::StoreOwner;
use crate::check::value::Input;

/// "name" or "name:argument" -> an input of the store vocabulary; `create:<name>:other` writes in another epoch.
fn seq(items: &[&str]) -> Vec<Input> {
    items.iter().map(|item| match item.split_once(':') {
        Some(("create", rest)) => match rest.split_once(':') {
            Some((name, "other")) => Input::new("create").with("as", name).with("epoch", "other"),
            _ => Input::new("create").with("as", rest),
        },
        _ => Input::new(item),
    }).collect()
}

/// The public calls of the real core under every property P-STORE-01..06: the traces of the findings, then every sequence to depth 4 and the seeded long ones.
#[test]
fn store_properties_hold_over_the_public_traces() {
    limited(LIMIT, async {
        let required = [
            seq(&["login", "create:a", "session_reset", "login", "create:b:other", "rows"]),
            seq(&["login", "create:a", "close", "create:b"]),
            seq(&["login", "create:a", "session_reset", "login", "create:b", "create:c:other", "rows"]),
            seq(&["login", "close", "create:a", "create:b:other"]),
        ];
        if let Err(divergence) = explore_inputs(&StoreOwner::public(None), &required).await {
            panic!("{divergence}");
        }
    });
}
