//! Direction B: the owners `Windows`, `Feed`, `Deliveries` and the runners of their effects.
//! A runner does IO and answers with an input of its owner. It decides no transition.

pub(crate) mod codec;
pub mod deliveries;
pub mod feed;
pub mod windows;

mod delivery_io;
mod feed_codec;
mod feed_io;
mod ports;
mod schema_io;
mod snapshots;
mod window_io;

pub use delivery_io::DeliveryIo;
pub use feed_codec::SchemaFeedCodec;
pub use feed_io::{FeedCodec, FeedIo};
pub use ports::{Admit, Refused, Rest};
pub use schema_io::{max_pages, DEFAULT_PAGE_SIZE, preload_keys, preload_sources, refreshes_on_foreground, ReadOperation, ReadOperations, SchemaWindowCodec};
pub use snapshots::{DbSnapshots, SnapshotStatements, Snapshots, SqliteSnapshots};
pub use window_io::{WindowCodec, WindowIo};

#[cfg(test)]
mod delivery_io_tests;
#[cfg(test)]
mod feed_codec_tests;
#[cfg(test)]
mod feed_io_tests;
#[cfg(test)]
mod rig;
#[cfg(test)]
mod schema_io_tests;
#[cfg(test)]
mod window_io_tests;
