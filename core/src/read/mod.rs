mod connection;
mod diff;
mod landing;

pub use connection::{connection_nodes, connection_page_info, connection_rows, EDGE_KEY, landing_fault, LandingFault};
pub use diff::{Diff, Entry, Stamp};
pub(crate) use diff::longest_increasing;
pub use landing::{incoming_ids, plan_landing, LandingError, LandingPlan, StoredScope};

#[cfg(test)]
mod connection_tests;
#[cfg(test)]
mod landing_tests;
