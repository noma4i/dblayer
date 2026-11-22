use super::cloud::{Cloud, CloudEffect, CloudInput, CloudState};
use crate::runtime::{Effect, StateOwner, Time};

#[test]
fn cloud_account_replacement_waits_for_erase_and_discards_the_old_read() {
    let mut state = CloudState::configured(3, None, &crate::ffi::CloudBackend::Drive, &crate::ffi::CloudApplication::Standalone { namespace: "notes".into() });
    let mut step = |input| {
        Cloud::step(&mut state, input, Time(0)).into_iter().filter_map(|effect| match effect {
            Effect::Run(effect) => Some(effect),
            Effect::Cancel(_) => None,
            _ => panic!("unexpected cloud control effect"),
        }).collect::<Vec<_>>()
    };
    let account = |key: &str| CloudInput::Provider(crate::ffi::CloudAccount::Available { key: key.into() });
    assert!(step(account("first")).is_empty());
    let effects = step(CloudInput::Online(true));
    let [CloudEffect::Start(first)] = effects.as_slice() else { panic!("expected the first read: {effects:?}") };
    assert!(!first.domain.is_empty());
    let effects = step(account("second"));
    let [CloudEffect::Stop { generation }] = effects.as_slice() else {
        panic!("expected stop before erase: {effects:?}")
    };
    assert_eq!(*generation, first.generation);
    assert!(step(account("third")).is_empty());
    assert!(step(CloudInput::InitialRead { generation: first.generation }).is_empty());
    let effects = step(CloudInput::Stopped { generation: first.generation });
    let [CloudEffect::Erase { domain, token }] = effects.as_slice() else { panic!("expected erase after stop: {effects:?}") };
    assert_eq!(domain, &first.domain);
    assert!(step(CloudInput::Erased { token: token + 1 }).is_empty());
    let effects = step(CloudInput::Erased { token: *token });
    let [CloudEffect::Start(third)] = effects.as_slice() else { panic!("expected new scope: {effects:?}") };
    assert_ne!(third.domain, first.domain);
    assert!(third.generation > first.generation);
    assert_eq!(step(CloudInput::InitialRead { generation: third.generation }), vec![CloudEffect::Bootstrap(third.clone())]);
    assert!(step(CloudInput::BootstrapDone { generation: first.generation }).is_empty());
    assert_eq!(step(CloudInput::BootstrapDone { generation: third.generation }), vec![CloudEffect::Ready(third.clone()), CloudEffect::Send(third.clone())]);
    assert_eq!(step(CloudInput::Provider(crate::ffi::CloudAccount::Unavailable)), vec![CloudEffect::Stop { generation: third.generation }]);
    assert!(step(CloudInput::Online(true)).is_empty());
    assert!(step(CloudInput::Close).is_empty());
    assert!(step(CloudInput::Stopped { generation: first.generation }).is_empty());
    assert_eq!(step(CloudInput::Stopped { generation: third.generation }), vec![CloudEffect::Drained]);
}
