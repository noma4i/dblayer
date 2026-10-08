#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadMoreVerdict {
    FetchNextPage,
    NoCursor,
    MaxPagesReached,
    AlreadyLoading,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PageChain {
    pub cursor: Option<String>,
    pub has_next: bool,
    pub loaded_pages: usize,
    pub is_loading: bool,
}

impl PageChain {
    pub fn advanced(&self, next_cursor: Option<String>, has_next: bool) -> Self {
        Self { cursor: next_cursor, has_next, loaded_pages: self.loaded_pages + 1, is_loading: false }
    }
}

impl LoadMoreVerdict {
    pub fn of(chain: &PageChain, max_pages: Option<usize>) -> Self {
        if chain.is_loading {
            return LoadMoreVerdict::AlreadyLoading;
        }
        if !chain.has_next || chain.cursor.as_deref().is_none_or(str::is_empty) {
            return LoadMoreVerdict::NoCursor;
        }
        if max_pages.is_some_and(|limit| chain.loaded_pages >= limit) {
            return LoadMoreVerdict::MaxPagesReached;
        }
        LoadMoreVerdict::FetchNextPage
    }

    pub fn calls_transport(self) -> bool {
        self == LoadMoreVerdict::FetchNextPage
    }
}
