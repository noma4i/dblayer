use std::sync::{mpsc, Mutex};
use std::thread::ThreadId;

use super::{Config, Database};
use crate::test_wait::{join_within, recv_within};

struct PausedOpen {
    path: String,
    entered: mpsc::Sender<ThreadId>,
    release: mpsc::Receiver<()>,
}

static OPEN: Mutex<Option<PausedOpen>> = Mutex::new(None);

unsafe extern "C" fn pause_open(db: *mut rusqlite::ffi::sqlite3, _: *mut *mut std::ffi::c_char, _: *const rusqlite::ffi::sqlite3_api_routines) -> i32 {
    rusqlite::ffi::sqlite3_progress_handler(db, 1, Some(pause_statement), db.cast());
    rusqlite::ffi::SQLITE_OK
}

unsafe extern "C" fn pause_statement(context: *mut std::ffi::c_void) -> i32 {
    let db = context.cast::<rusqlite::ffi::sqlite3>();
    let filename = rusqlite::ffi::sqlite3_db_filename(db, c"main".as_ptr());
    if !filename.is_null() {
        rusqlite::ffi::sqlite3_progress_handler(db, 0, None, std::ptr::null_mut());
        let mut slot = OPEN.lock().unwrap();
        if slot.as_ref().is_some_and(|pause| std::ffi::CStr::from_ptr(filename).to_string_lossy() == pause.path) {
            let pause = slot.take().unwrap();
            drop(slot);
            pause.entered.send(std::thread::current().id()).unwrap();
            recv_within(&pause.release);
        }
    }
    rusqlite::ffi::SQLITE_OK
}

#[test]
fn opening_leaves_the_calling_thread_free_while_sqlite_is_busy() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().canonicalize().unwrap().join("paused.sqlite").to_string_lossy().into_owned();
    let (entered, opened) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (caller, caller_id) = mpsc::channel();
    let (free, freed) = mpsc::channel();
    *OPEN.lock().unwrap() = Some(PausedOpen { path: path.clone(), entered, release: released });
    unsafe { rusqlite::auto_extension::register_auto_extension(pause_open).unwrap(); }
    let thread = std::thread::spawn(move || {
        caller.send(std::thread::current().id()).unwrap();
        let mut opening = Box::pin(Database::open(Config { path: Some(path), data_version: None, cache_rows: 8 }, "[]".into(), crate::ffi::OpenPorts::default()));
        let pending = matches!(opening.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Pending);
        free.send(pending).unwrap();
        if pending { super::database::executor().unwrap().block_on(opening).unwrap(); }
    });
    let caller = recv_within(&caller_id);
    let worker = opened.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
    let free = if worker != caller { freed.recv_timeout(std::time::Duration::from_secs(10)).unwrap() } else { false };
    release.send(()).unwrap();
    join_within(thread);
    rusqlite::auto_extension::cancel_auto_extension(pause_open);
    assert_ne!(worker, caller, "SQLite opening ran on the calling thread");
    assert!(free, "the caller must return Pending while SQLite remains paused");
}

#[tokio::test]
async fn an_opening_error_leaves_the_same_path_available_for_retry() {
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().join("blocked");
    std::fs::write(&parent, b"file").unwrap();
    let path = parent.join("database.sqlite");
    let config = || Config { path: Some(path.to_string_lossy().into_owned()), data_version: None, cache_rows: 8 };
    assert!(Database::open(config(), "[]".into(), crate::ffi::OpenPorts::default()).await.is_err());
    std::fs::remove_file(parent).unwrap();
    let database = Database::open(config(), "[]".into(), crate::ffi::OpenPorts::default()).await.unwrap();
    assert!(path.is_file());
    assert!(database.open_report().recreated.is_none());
}
