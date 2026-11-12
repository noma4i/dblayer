use super::*;

impl Delivery {
    pub(super) fn new(epoch: Epoch, view: View, stage: DeliveryStage, revision: i64) -> Self {
        Self {
            epoch,
            view,
            stage,
            read_revision: revision,
            read_id: 0,
            read_inflight: false,
            read_seen: 0,
            read_position: 0,
            revision: 0,
            value_position: 0,
            acknowledged: None,
            resync_pending: false,
            resync_failed: false,
            read_error: None,
            rows: Vec::new(),
            acked_rows: Vec::new(),
            temporary: BTreeMap::new(),
            count: None,
            operations: None,
            total: None,
            window: WindowState::idle(),
            held_state: None,
            next_feed: None,
            position: 0,
            ready: false,
            held_events: Vec::new(),
            blob: None,
            transfer: None,
            upload_row: None,
            status_revision: 0,
            upload_inflight: false,
            journal: VecDeque::new(),
            waiting: None,
            ended_given: false,
            cause: EndCause::Cancelled,
            replies: Vec::new(),
            closing: false,
            released: false,
            reader: None,
            sink_closed: false,
            interested: false,
            wants: false,
            pending_read: None,
            deferred: None,
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
            Kind::Changes | Kind::CableCloses => {
                machine.stage = DeliveryStage::Live;
                machine.push(Value::Ready, fx);
            }
            Kind::Blob => {
                fx.push(Emit::Run(StreamEffect::StatusInterest { plus: true, key: machine.view.subject.clone() }));
                machine.interested = true;
            }
            Kind::List | Kind::Count | Kind::Operations | Kind::Entity { .. } => {
                fx.push(machine.new_read(revision));
                machine.watch(fx);
            }
        }
        machine
    }

    /// The subscription of the model and the interest in the window of the view, once.
    pub(super) fn watch(&mut self, fx: &mut Vec<Emit>) {
        let kind = self.view.kind;
        if self.view.live {
            fx.push(Emit::WantPlus);
            self.wants = true;
        }
        let watches = self.view.remote && matches!(kind, Kind::List | Kind::Count | Kind::Entity { .. });
        if watches {
            fx.push(if matches!(kind, Kind::Entity { fetch: Fetch::IfMissing | Fetch::Never }) { Emit::InterestPassive } else { Emit::InterestPlus });
            if self.view.reload { fx.push(Emit::ReloadOnConnect); }
            self.interested = true;
            if kind == (Kind::Entity { fetch: Fetch::Force }) {
                fx.push(Emit::Load { force: true });
            }
        }
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
