use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

pub(super) trait Closable: Send + Sync {
    fn close(&self);
    fn registration(&self) -> &OnceLock<Registration>;
    fn busy(&self) -> bool { false }
    fn reset(&self, _models: Option<&[String]>) {}
    fn resets(&self) -> bool { false }
}

pub(super) struct Registration { streams: Weak<OpenStreams>, id: usize }

impl Registration {
    pub(super) fn remove(&self) {
        if let Some(streams) = self.streams.upgrade() {
            let removed = {
                let mut members = crate::db::locked(&streams.members);
                members.resets.remove(&self.id);
                members.open.remove(&self.id)
            };
            drop(removed);
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) { self.remove(); }
}

#[derive(Default)]
struct Members {
    closed: bool,
    open: HashMap<usize, Weak<dyn Closable>>,
    resets: HashMap<usize, Weak<dyn Closable>>,
}

#[derive(Default)]
pub(super) struct OpenStreams {
    members: Mutex<Members>,
    #[cfg(test)]
    work: std::sync::atomic::AtomicUsize,
}

impl OpenStreams {
    pub(super) fn reset(&self, models: Option<&[String]>) {
        let open: Vec<_> = crate::db::locked(&self.members).resets.values().cloned().collect();
        for member in open.into_iter().filter_map(|member| member.upgrade()) { member.reset(models); }
    }

    pub(super) fn busy(&self) -> bool {
        let open: Vec<_> = crate::db::locked(&self.members).open.values().cloned().collect();
        open.into_iter().filter_map(|member| member.upgrade()).any(|member| member.busy())
    }

    pub(super) fn enroll<M: Closable + 'static>(self: &Arc<Self>, member: &Arc<M>) {
        assert!(member.registration().get().is_none());
        let registration = {
            let mut members = crate::db::locked(&self.members);
            if !members.closed {
                let id = Arc::as_ptr(member) as usize;
                members.open.insert(id, Arc::downgrade(member) as Weak<dyn Closable>);
                if member.resets() { members.resets.insert(id, Arc::downgrade(member) as Weak<dyn Closable>); }
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
            members.resets.clear();
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
