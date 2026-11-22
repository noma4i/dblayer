use super::cloud::{Cloud, CloudEffect, CloudInput, CloudState};
use crate::runtime::{Effect, StateOwner, Time};

#[test]
fn cloud_account_replacement_waits_for_erase_and_discards_the_old_read() {
    let mut state = CloudState::configured(3, None, &crate::ffi::CloudBackend::Drive, &crate::ffi::CloudApplication::Standalone { namespace: "notes".into() });
    let mut step = |input| {
        Cloud::step(&mut state, input, Time(0)).into_iter().filter_map(|effect| match effect {
            Effect::Run(CloudEffect::Publish(_)) => None,
            Effect::Run(effect) => Some(effect),
            Effect::Cancel(_) => None,
            _ => panic!("unexpected cloud control effect"),
        }).collect::<Vec<_>>()
    };
    let initializing = step(CloudInput::Initialize { reply: 1 });
    let [CloudEffect::Observe { observation }] = initializing.as_slice() else { panic!("expected provider observation") };
    let observation = *observation;
    step(CloudInput::Observed { observation });
    let account = |key: &str| CloudInput::Provider { observation, account: crate::ffi::CloudAccount::Available { key: key.into() } };
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
    let [CloudEffect::Erase { domain, token, epoch: 3 }] = effects.as_slice() else { panic!("expected erase after stop: {effects:?}") };
    assert_eq!(domain, &first.domain);
    assert!(step(CloudInput::Erased { token: token + 1 }).is_empty());
    let effects = step(CloudInput::Erased { token: *token });
    let [CloudEffect::Start(third)] = effects.as_slice() else { panic!("expected new scope: {effects:?}") };
    assert_ne!(third.domain, first.domain);
    assert!(third.generation > first.generation);
    assert_eq!(step(CloudInput::InitialRead { generation: third.generation }), vec![CloudEffect::Bootstrap(third.clone())]);
    assert!(step(CloudInput::BootstrapDone { generation: first.generation }).is_empty());
    assert_eq!(step(CloudInput::BootstrapDone { generation: third.generation }), vec![CloudEffect::Ready(third.clone()), CloudEffect::Send(third.clone())]);
    assert_eq!(step(CloudInput::Provider { observation, account: crate::ffi::CloudAccount::Unavailable }), vec![CloudEffect::Stop { generation: third.generation }]);
    assert!(step(CloudInput::Online(true)).is_empty());
    assert!(step(CloudInput::Close).is_empty());
    assert!(step(CloudInput::Stopped { generation: first.generation }).is_empty());
    assert_eq!(step(CloudInput::Stopped { generation: third.generation }), vec![CloudEffect::Unobserve { observation }]);
    assert_eq!(step(CloudInput::Unobserved { observation }), vec![CloudEffect::Drained]);
}

#[test]
fn cloud_disable_waits_for_account_erase_to_finish() {
    use crate::ffi::{CloudAccount, CloudApplication, CloudBackend};

    let mut state = CloudState::configured(3, None, &CloudBackend::Drive, &CloudApplication::Standalone { namespace: "notes".into() });
    let mut step = |input| Cloud::step(&mut state, input, Time(0)).into_iter().filter_map(|effect| match effect {
        Effect::Run(CloudEffect::Publish(_)) => None,
        Effect::Run(effect) => Some(effect),
        _ => None,
    }).collect::<Vec<_>>();
    let initial = step(CloudInput::Initialize { reply: 1 });
    let [CloudEffect::Observe { observation }] = initial.as_slice() else { panic!("expected provider observation") };
    let observation = *observation;
    step(CloudInput::Observed { observation });
    step(CloudInput::Provider { observation, account: CloudAccount::Available { key: "provider".into() } });
    let start = step(CloudInput::Online(true));
    let [CloudEffect::Start(scope)] = start.as_slice() else { panic!("expected cloud start") };
    step(CloudInput::Provider { observation, account: CloudAccount::SignedOut });
    let erase = step(CloudInput::Stopped { generation: scope.generation });
    let [CloudEffect::Erase { token, .. }] = erase.as_slice() else { panic!("expected account erase") };
    assert_eq!(step(CloudInput::SetEnabled { enabled: false, reply: 2 }), [CloudEffect::WriteEnabled(false)]);
    assert_eq!(step(CloudInput::PreferenceWritten(Ok(()))), [CloudEffect::Unobserve { observation }]);
    assert!(step(CloudInput::Unobserved { observation }).is_empty());
    assert_eq!(step(CloudInput::Erased { token: *token }), [CloudEffect::Reply { id: 2, answer: Ok(false) }]);
}

#[test]
fn cloud_relay_suspension_retires_its_timer_and_a_live_retry_renews_the_scope() {
    use crate::ffi::{CloudAccount, CloudApplication, CloudBackend, CloudRelayEvent};
    use crate::runtime::ScheduleId;
    use std::time::Duration;

    let mut state = CloudState::configured(3, None, &CloudBackend::Drive, &CloudApplication::Standalone { namespace: "notes".into() });
    let mut step = |input| Cloud::step(&mut state, input, Time(0)).into_iter()
        .filter(|effect| !matches!(effect, Effect::Run(CloudEffect::Publish(_)))).collect::<Vec<_>>();
    let initializing = step(CloudInput::Initialize { reply: 1 });
    let [Effect::Run(CloudEffect::Observe { observation })] = initializing.as_slice() else { panic!("expected provider observation") };
    let observation = *observation;
    step(CloudInput::Observed { observation });
    assert!(step(CloudInput::Provider { observation, account: CloudAccount::Available { key: "provider".into() } }).is_empty());
    let starting = step(CloudInput::Online(true));
    let [Effect::Run(CloudEffect::Start(scope))] = starting.as_slice() else { panic!("expected cloud start") };
    let relay = |event| CloudInput::Relay { scope: scope.clone(), event };
    let leased = step(relay(CloudRelayEvent::Lease { milliseconds: 71 }));
    let [Effect::Schedule { id: ScheduleId(1), after, input: CloudInput::RelayDue { generation, token } }] = leased.as_slice() else { panic!("expected lease renewal") };
    assert_eq!(*after, Duration::from_millis(71));
    assert!(matches!(step(relay(CloudRelayEvent::Suspended)).as_slice(), [Effect::Cancel(ScheduleId(1))]));
    assert!(step(CloudInput::RelayDue { generation: *generation, token: *token }).is_empty());

    let ended = step(relay(CloudRelayEvent::Ended));
    let [Effect::Backoff { id: ScheduleId(1), attempt: 1, input: CloudInput::RelayDue { generation, token }, .. }] = ended.as_slice() else { panic!("expected a bounded relay retry") };
    assert!(matches!(step(CloudInput::RelayDue { generation: *generation, token: *token }).as_slice(),
        [Effect::Cancel(ScheduleId(0)), Effect::Cancel(ScheduleId(1)), Effect::Run(CloudEffect::Stop { generation })] if *generation == scope.generation));
    let restarted = step(CloudInput::Stopped { generation: scope.generation });
    let [Effect::Run(CloudEffect::Start(current))] = restarted.as_slice() else { panic!("expected a new generation") };
    assert_eq!(current.domain, scope.domain);
    assert!(current.generation > scope.generation);
    assert!(step(relay(CloudRelayEvent::Ended)).is_empty());
}
