use super::*;

impl Delivery {
    pub(super) fn new(epoch: Epoch, view: View, stage: DeliveryStage, revision: i64) -> Self {
        let window = if view.remote { WindowState::idle() } else { WindowState { phase: WindowPhase::Loaded, error: None, loaded: None, has_more: false, settled: true, total: None } };
        Self {
            opened_at: 0,
            epoch,
            view,
            stage,
            read_revision: revision,
            read_id: 0,
            read_seen: 0,
            read_position: 0,
            reading_position: 0,
            revision: 0,
            acknowledged: None,
            resync_pending: false,
            resync_failed: false,
            read_error: false,
            rows: Vec::new(),
            acked_rows: Vec::new(),
            temporary: BTreeMap::new(),
            count: None,
            operations: None,
            total: None,
            window,
            held_state: None,
            next_feed: None,
            position: 0,
            ready: false,
            held_events: Vec::new(),
            blob: None,
            retry: None,
            status_revision: 0,
            journal: VecDeque::new(),
            waiting: None,
            ended_given: false,
            cause: EndCause::Cancelled,
            replies: Vec::new(),
            closing: false,
            released: false,
            sink_closed: false,
            interested: false,
            wants: false,
            pending_read: None,
        }
    }

    pub(super) fn open(view: View, epoch: Epoch, revision: i64, fx: &mut Vec<Emit>) -> Self {
        let kind = view.kind;
        let mut machine = Self::new(epoch, view, DeliveryStage::Opening, revision);
        match kind {
            Kind::Events => {
                machine.stage = DeliveryStage::Live;
                fx.push(Emit::WantPlus);
                machine.interested = true;
            }
            Kind::Changes => {
                machine.stage = DeliveryStage::Live;
                machine.push(Value::Ready, fx);
            }
            Kind::Blob | Kind::RetryStatus => {
                fx.push(Emit::Run(StreamEffect::StatusInterest { plus: true }));
                machine.interested = true;
            }
            Kind::List | Kind::Count | Kind::Operations | Kind::Entity { .. } => {
                fx.push(machine.new_read(revision));
                if machine.view.live {
                    fx.push(Emit::WantPlus);
                    machine.wants = true;
                }
                let watches = machine.view.remote
                    && matches!(kind, Kind::List | Kind::Count | Kind::Entity { fetch: Fetch::Stale | Fetch::Force | Fetch::IfMissing });
                if watches {
                    fx.push(Emit::InterestPlus);
                    if machine.view.reload { fx.push(Emit::ReloadOnConnect); }
                    machine.interested = true;
                    if kind == (Kind::Entity { fetch: Fetch::Force }) {
                        fx.push(Emit::Load { force: true });
                    }
                }
            }
        }
        machine
    }

    pub(super) fn ended(view: View, epoch: Epoch, cause: EndCause, fx: &mut Vec<Emit>) -> Self {
        let mut machine = Self::new(epoch, view, DeliveryStage::Ended, 0);
        machine.journal.push_back(Value::Ended(cause.clone()));
        machine.cause = cause;
        machine.sink_closed = true;
        fx.push(Emit::Run(StreamEffect::Available));
        machine
    }
}
