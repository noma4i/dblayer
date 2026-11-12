#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Owner {
    Network,
    Session,
    Window,
    Feed,
    Delivery,
    Outbox,
    Transfer,
    Calls,
    LocalWrites,
    Rounds,
}
