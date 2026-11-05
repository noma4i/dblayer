//! Direction B: the owners `Windows`, `Feed`, `Deliveries` and the runners of their effects (`design.md` F3).
//! A runner does IO and answers with an input of its owner. It decides no transition.

pub mod deliveries;
pub mod feed;
pub mod windows;

mod delivery_io;
mod feed_io;
mod ports;
mod snapshots;
mod window_io;

pub use delivery_io::{DeliveryIo, StreamTable};
pub use feed_io::{FeedCodec, FeedIo};
pub use ports::{Admit, Rest, StorePort};
pub use snapshots::{SnapshotStatements, Snapshots, SqliteSnapshots};
pub use window_io::{WindowCodec, WindowIo};

#[cfg(test)]
mod delivery_io_tests;
#[cfg(test)]
mod feed_io_tests;
#[cfg(test)]
mod rig;
#[cfg(test)]
mod window_io_tests;

use super::lifecycle::Peer;

pub const ABSENT: [Peer; 3] = [Peer::Windows, Peer::Feed, Peer::Deliveries];
