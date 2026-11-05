//! Direction B installs Windows, Feed and Deliveries here. Until then these peers answer `Drained` through `ABSENT`.

use super::lifecycle::Peer;

pub const ABSENT: [Peer; 3] = [Peer::Windows, Peer::Feed, Peer::Deliveries];
