use std::sync::{Arc, Mutex, Weak};

pub(super) trait Closable: Send + Sync {
    fn close(&self);
}

#[derive(Default)]
struct Members {
    closed: bool,
    open: Vec<Weak<dyn Closable>>,
}

#[derive(Default)]
pub(super) struct OpenStreams {
    members: Mutex<Members>,
}

impl OpenStreams {
    pub(super) fn enroll<M: Closable + 'static>(&self, member: &Arc<M>) {
        let late = {
            let mut members = crate::db::locked(&self.members);
            if !members.closed {
                members.open.retain(|held| held.strong_count() > 0);
                members.open.push(Arc::downgrade(member) as Weak<dyn Closable>);
            }
            members.closed
        };
        if late {
            member.close();
        }
    }

    pub(super) fn close(&self) {
        let open = {
            let mut members = crate::db::locked(&self.members);
            if std::mem::replace(&mut members.closed, true) {
                return;
            }
            std::mem::take(&mut members.open)
        };
        for member in open.iter().filter_map(Weak::upgrade) {
            member.close();
        }
    }

    #[cfg(test)]
    pub(super) fn enrolled(&self) -> usize {
        crate::db::locked(&self.members).open.iter().filter(|held| held.strong_count() > 0).count()
    }
}
