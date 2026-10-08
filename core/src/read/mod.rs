mod attach;
mod chain;
mod connection;
mod coverage;
mod feed;
mod landing;
mod load_more;
mod loading;
mod thread;

pub use attach::{AttachFetch, AttachResult, DetachResult};
pub use chain::{chain_shrinks, surviving_ids, ChainVerdict};
pub use connection::{
    connection_nodes, connection_page_info, connection_total_count, is_landable, landing_fault, LandingFault,
};
pub use coverage::{coverage_change, coverage_of, drops_missing, replaces_membership};
pub use feed::{
    feed_invalidation, feed_next_baseline, moves_feed, Coordinate, FEED_MODEL, FEED_RELATION, MOVE_THRESHOLD_METERS,
};
pub use landing::{plan_landing, LandingError, LandingPlan, StoredMember};
pub use load_more::{LoadMoreVerdict, PageChain};
pub use loading::{LoadingPhase, LoadingState};
pub use thread::{ThreadRow, THREAD_ORDER_FIELDS};

#[cfg(test)]
mod attach_tests;
#[cfg(test)]
mod chain_tests;
#[cfg(test)]
mod connection_tests;
#[cfg(test)]
mod coverage_tests;
#[cfg(test)]
mod feed_tests;
#[cfg(test)]
mod landing_tests;
#[cfg(test)]
mod load_more_tests;
#[cfg(test)]
mod loading_tests;
#[cfg(test)]
mod thread_tests;
