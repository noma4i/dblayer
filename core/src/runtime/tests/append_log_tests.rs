use super::AppendLog;

#[test]
fn pushes_from_many_threads_are_all_listed_and_keep_the_order_of_each_thread() {
    let log = AppendLog::default();
    std::thread::scope(|scope| {
        for thread in 0..8u32 {
            let log = &log;
            scope.spawn(move || (0..200u32).for_each(|index| log.push((thread, index))));
        }
    });
    let listed: Vec<(u32, u32)> = log.iter().copied().collect();
    assert_eq!(listed.len(), 8 * 200, "no push is lost");
    for thread in 0..8 {
        let own: Vec<u32> = listed.iter().filter(|(owner, _)| *owner == thread).map(|(_, index)| *index).collect();
        assert_eq!(own, (0..200).collect::<Vec<u32>>(), "the pushes of one thread keep their order");
    }
}
