use crate::test_wait::limited;

const LIMIT: std::time::Duration = std::time::Duration::from_secs(20);
use super::lifecycle::{explore, LifecycleOwner};
use crate::check::value::Input;

/// "name" or "name:argument" -> an input of the lifecycle vocabulary.
fn seq(items: &[&str]) -> Vec<Input> {
    items.iter().map(|item| match item.split_once(':') {
        Some(("call_data", epoch)) => Input::new("call_data").with("epoch", epoch),
        Some(("create", name)) => Input::new("create").with("as", name),
        _ => Input::new(item),
    }).collect()
}

/// The public calls of the real core under every property P-LIFE-01..08: the traces of the findings, then every sequence to depth 4 and the seeded long ones.
#[test]
fn life_properties_hold_over_the_public_traces() {
    limited(LIMIT, async {
        let required = [
            seq(&["login", "create:a", "session_reset", "login", "rows"]),
            seq(&["login", "session_reset", "session_reset"]),
            seq(&["login", "close", "call_data:current", "close"]),
            seq(&["login", "close", "session_reset", "close", "login"]),
            seq(&["login", "idle", "session_reset", "idle"]),
            seq(&["login", "call_data:other"]),
        ];
        if let Err(divergence) = explore(&LifecycleOwner::public(None), 4, &[1, 2], 30, &required).await {
            panic!("{divergence}");
        }
    });
}
