use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

pub(super) trait Closable: Send + Sync {
    fn close(&self);
    fn registration(&self) -> &OnceLock<Registration>;
}

pub(super) struct Registration { streams: Weak<OpenStreams>, id: usize }

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(streams) = self.streams.upgrade() {
            let removed = crate::db::locked(&streams.members).open.remove(&self.id);
            drop(removed);
        }
    }
}

#[derive(Default)]
struct Members {
    closed: bool,
    open: HashMap<usize, Weak<dyn Closable>>,
}

#[derive(Default)]
pub(super) struct OpenStreams {
    members: Mutex<Members>,
    #[cfg(test)]
    work: std::sync::atomic::AtomicUsize,
}

impl OpenStreams {
    pub(super) fn enroll<M: Closable + 'static>(self: &Arc<Self>, member: &Arc<M>) {
        assert!(member.registration().get().is_none());
        let registration = {
            let mut members = crate::db::locked(&self.members);
            if !members.closed {
                let id = Arc::as_ptr(member) as usize;
                members.open.insert(id, Arc::downgrade(member) as Weak<dyn Closable>);
                Some(Registration { streams: Arc::downgrade(self), id })
            } else {
                None
            }
        };
        if let Some(registration) = registration {
            assert!(member.registration().set(registration).is_ok());
        } else {
            member.close()
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
        for member in open.values().filter_map(Weak::upgrade) {
            #[cfg(test)]
            self.work.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            member.close();
        }
    }

    #[cfg(test)]
    pub(super) fn enrolled(&self) -> usize {
        crate::db::locked(&self.members).open.len()
    }

    #[cfg(test)]
    pub(super) fn work(&self) -> usize { self.work.load(std::sync::atomic::Ordering::SeqCst) }
}
