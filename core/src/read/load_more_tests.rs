use super::*;

fn chain(cursor: Option<&str>, has_next: bool, loaded_pages: usize, is_loading: bool) -> PageChain {
    PageChain { cursor: cursor.map(str::to_string), has_next, loaded_pages, is_loading }
}

#[test]
fn verdict_follows_cursor_max_pages_and_loading_state() {
    let ready = chain(Some("c1"), true, 0, false);
    let at_limit = chain(Some("c1"), true, 10, false);
    let loading = chain(Some("c1"), true, 5, true);
    assert_eq!(LoadMoreVerdict::of(&ready, None), LoadMoreVerdict::FetchNextPage);
    assert_eq!(LoadMoreVerdict::of(&PageChain::default(), None), LoadMoreVerdict::NoCursor);
    assert_eq!(LoadMoreVerdict::of(&chain(Some(""), true, 0, false), None), LoadMoreVerdict::NoCursor);
    assert_eq!(LoadMoreVerdict::of(&chain(Some("c1"), false, 0, false), None), LoadMoreVerdict::NoCursor);
    assert_eq!(LoadMoreVerdict::of(&at_limit, Some(10)), LoadMoreVerdict::MaxPagesReached);
    assert_eq!(LoadMoreVerdict::of(&at_limit, Some(11)), LoadMoreVerdict::FetchNextPage);
    assert_eq!(LoadMoreVerdict::of(&loading, Some(1)), LoadMoreVerdict::AlreadyLoading);
}

#[test]
fn advancing_counts_the_page_and_clears_loading() {
    let advanced = chain(Some("c1"), true, 2, true).advanced(Some("c2".into()), false);
    assert_eq!(advanced, chain(Some("c2"), false, 3, false));
}
