use super::cloud::{Cloud, CloudEffect, CloudInput, CloudState};
use crate::runtime::{Effect, StateOwner, Time};

#[test]
fn cloud_account_replacement_waits_for_erase_and_discards_the_old_read() {
    let mut state = CloudState::new(3);
    let mut step = |input| {
        Cloud::step(&mut state, input, Time(0)).into_iter().map(|effect| match effect {
            Effect::Run(effect) => effect,
            _ => panic!("unexpected cloud control effect"),
        }).collect::<Vec<_>>()
    };
    let account = |domain: &str| CloudInput::Account { epoch: 3, domain: Some(domain.into()) };
    assert!(step(account("first")).is_empty());
    let effects = step(CloudInput::Online(true));
    let [CloudEffect::Start(first)] = effects.as_slice() else { panic!("expected the first read: {effects:?}") };
    assert_eq!(first.domain, "first");
    let effects = step(account("second"));
    let [CloudEffect::Stop { generation }, CloudEffect::Erase { domain, token }] = effects.as_slice() else {
        panic!("expected stop and erase: {effects:?}")
    };
    assert_eq!(*generation, first.generation);
    assert_eq!(domain, "first");
    assert!(step(account("third")).is_empty());
    assert!(step(CloudInput::InitialRead { generation: first.generation }).is_empty());
    assert!(step(CloudInput::Erased { token: token + 1 }).is_empty());
    assert!(step(CloudInput::Stopped { generation: first.generation }).is_empty());
    let effects = step(CloudInput::Erased { token: *token });
    let [CloudEffect::Start(third)] = effects.as_slice() else { panic!("expected new scope: {effects:?}") };
    assert_eq!(third.domain, "third");
    assert!(third.generation > first.generation);
    assert_eq!(step(CloudInput::InitialRead { generation: third.generation }), vec![CloudEffect::Ready(third.clone())]);
    assert_eq!(step(CloudInput::AccountUnavailable { epoch: 3 }), vec![CloudEffect::Stop { generation: third.generation }]);
    assert!(step(CloudInput::Online(true)).is_empty());
    assert!(step(CloudInput::Close).is_empty());
    assert!(step(CloudInput::Stopped { generation: first.generation }).is_empty());
    assert_eq!(step(CloudInput::Stopped { generation: third.generation }), vec![CloudEffect::Drained]);
}
