//! Direction C installs Outbox and Transfer here. Until then these peers answer `Drained` through `ABSENT`.

use super::lifecycle::Peer;

pub const ABSENT: [Peer; 2] = [Peer::Outbox, Peer::Transfer];
