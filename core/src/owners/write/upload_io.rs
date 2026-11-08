use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use base64::Engine;
use md5::{Digest, Md5};
use crate::transport::CancelToken;

const CHUNK: usize = 1024 * 1024;

pub async fn file_len(path: &Path, cancel: &CancelToken) -> io::Result<u64> {
    read_source(path, cancel, |path, _| {
        #[cfg(test)] super::tests::upload_io_tests::record_read(path, "metadata");
        Ok(std::fs::metadata(path)?.len())
    }).await
}

pub async fn copy_range(path: &Path, offset: u64, length: u64, target: &Path, cancel: &CancelToken) -> io::Result<()> {
    let target = target.to_path_buf();
    read_source(path, cancel, move |path, cancel| {
        #[cfg(test)] super::tests::upload_io_tests::record_read(path, "range");
        if std::fs::metadata(&target).is_ok_and(|stored| stored.len() == length) {
            return Ok(());
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut partial_name = target.as_os_str().to_os_string();
        partial_name.push(".partial");
        let partial = std::path::PathBuf::from(partial_name);
        let mut output = File::create(&partial)?;
        let mut buffer = vec![0; CHUNK];
        let mut left = length;
        while left > 0 {
            check_cancelled(cancel)?;
            let chunk = &mut buffer[..usize::try_from(left.min(CHUNK as u64)).map_err(|_| io::ErrorKind::InvalidInput)?];
            if let Err(error) = file.read_exact(chunk).and_then(|()| io::Write::write_all(&mut output, chunk)) {
                let _ = std::fs::remove_file(&partial);
                return Err(error);
            }
            left -= chunk.len() as u64;
        }
        std::fs::rename(&partial, &target)
    }).await
}

pub async fn md5_base64(path: &Path, cancel: &CancelToken) -> io::Result<String> {
    read_source(path, cancel, |path, cancel| {
        #[cfg(test)] super::tests::upload_io_tests::record_read(path, "checksum");
        let mut file = File::open(path)?;
        let mut hasher = Md5::new();
        let mut buffer = vec![0; CHUNK];
        loop {
            check_cancelled(cancel)?;
            let read = file.read(&mut buffer)?;
            if read == 0 { break; }
            hasher.update(&buffer[..read]);
        }
        Ok(base64::engine::general_purpose::STANDARD.encode(hasher.finalize()))
    }).await
}

fn check_cancelled(cancel: &CancelToken) -> io::Result<()> {
    if cancel.is_cancelled() { return Err(io::ErrorKind::Interrupted.into()); }
    Ok(())
}

async fn read_source<T: Send + 'static>(path: &Path, cancel: &CancelToken, read: impl FnOnce(&Path, &CancelToken) -> io::Result<T> + Send + 'static) -> io::Result<T> {
    let (path, cancel) = (path.to_path_buf(), cancel.clone());
    let awaiting = crate::runtime::running_launch();
    let read = crate::task::spawn_blocking(crate::runtime::running_launch(), move || {
        check_cancelled(&cancel)?;
        let value = read(&path, &cancel)?;
        check_cancelled(&cancel)?;
        Ok(value)
    }).await.map_err(io::Error::other)?;
    drop(awaiting);
    read
}
