//! Direction B: the owners `Windows`, `Feed`, `Deliveries` and the runners of their effects.
//! A runner does IO and answers with an input of its owner. It decides no transition.

pub(crate) mod codec;
pub mod diff;
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
pub use feed_codec::{channels_of, scoped_channels, SchemaFeedCodec};
pub use feed_io::{FeedCodec, FeedIo};
pub(crate) use feed_io::CLOSE_DRAIN;
pub use ports::{Admit, Refused, Rest};
#[cfg(test)]
pub(crate) use ports::{RequestsOnTheWay, InFlight};
pub use schema_io::{has_local_order, has_media, lifetimes, max_pages, members_complete, DEFAULT_PAGE_SIZE, preload_keys, preload_sources, refreshes_on_foreground, ReadOperation, ReadOperations, SchemaWindowCodec};
pub use snapshots::{DbSnapshots, Snapshots};
pub use window_io::{WindowCodec, WindowIo, WindowReader};

#[cfg(test)]
mod tests;
#[cfg(test)]
mod rig;
