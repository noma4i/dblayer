use super::*;
use std::collections::BTreeSet;
use crate::commit::WriteOrigin;

#[test]
fn replace_and_origin_rules_follow_policy() {
    let policies = [
        (WritePolicy::Server, false),
        (WritePolicy::Local, true),
        (WritePolicy::Continuity, true),
        (WritePolicy::Snapshot, true),
        (WritePolicy::Monotonic(Condition::NonEmpty), false),
        (WritePolicy::Keys(vec![(Path::new("id"), KeyPolicy::Server)].into_iter().collect(), Box::new(WritePolicy::Local)), true),
    ];
    for (policy, expected) in &policies {
        assert_eq!(applies_on_replace(policy), *expected);
    }
    assert_eq!(default_origins(&WritePolicy::Monotonic(Condition::NonEmpty)), [WriteOrigin::Snapshot, WriteOrigin::Event, WriteOrigin::Response]);
    assert_eq!(default_origins(&WritePolicy::Server), WriteOrigin::ALL);
    assert_eq!(allowed_origins_for_monotonic(&WriteOrigin::ALL), [WriteOrigin::Snapshot, WriteOrigin::Event, WriteOrigin::Response, WriteOrigin::Patch]);
    assert!(allowed_origins_for_monotonic(&[WriteOrigin::Optimistic]).is_empty());
}

#[test]
fn path_segments_and_condition_paths_walk_nested_cases() {
    let newer = Path::new("createdAt");
    let tuple = [Path::new("author.id"), Path::new("author.name")];
    let ladder = Path::new("rank");
    let condition = Condition::All(vec![
        Condition::NewerBy(newer.clone()),
        Condition::Any(vec![Condition::Tuple(tuple.to_vec()), Condition::NonEmpty]),
        Condition::Ladder(ladder.clone(), vec![vec!["server".into()], vec!["local".into(), "snapshot".into()]]),
    ]);
    assert_eq!(Path::new("media.transcodeStatus").segments(), ["media", "transcodeStatus"]);
    assert_eq!(Path::new("media.").segments(), ["media"]);
    let mut expected = vec![newer];
    expected.extend(tuple);
    expected.push(ladder);
    assert_eq!(condition_paths(&condition), expected);
}

#[test]
fn group_restores_left_to_right() {
    let group = WriteGroup::new(vec![Path::new("title")], vec![WritePolicy::Server, WritePolicy::Local, WritePolicy::Snapshot], None);
    assert_eq!(first_accepted_policy(&group, &BTreeSet::new()), Some(&WritePolicy::Server));
    assert_eq!(first_accepted_policy(&group, &[0].into()), Some(&WritePolicy::Local));
    assert_eq!(first_accepted_policy(&group, &[0, 1, 2].into()), None);
}

#[test]
fn default_policy_is_server_and_policies_round_trip_through_json() {
    assert_eq!(WritePolicy::default_policy(), WritePolicy::Server);
    let policy = WritePolicy::Keys(
        vec![(Path::new("a.b"), KeyPolicy::NonEmpty)].into_iter().collect(),
        Box::new(WritePolicy::Monotonic(Condition::Ladder(Path::new("rank"), vec![vec!["x".into()]]))),
    );
    let encoded = serde_json::to_string(&policy).unwrap();
    assert_eq!(serde_json::from_str::<WritePolicy>(&encoded).unwrap(), policy);
}
