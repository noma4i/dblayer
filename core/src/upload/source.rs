use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use base64::Engine;
use md5::{Digest, Md5};
use crate::transport::CancelToken;

const CHUNK: usize = 1024 * 1024;

pub async fn file_len(path: &Path, cancel: &CancelToken) -> io::Result<u64> {
    read_source(path, cancel, |path, _| {
        #[cfg(test)] super::source_tests::record_read(path, "metadata");
        Ok(std::fs::metadata(path)?.len())
    }).await
}

pub async fn read_range(path: &Path, offset: u64, length: u64, cancel: &CancelToken) -> io::Result<Vec<u8>> {
    read_source(path, cancel, move |path, cancel| {
        #[cfg(test)] super::source_tests::record_read(path, "range");
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; usize::try_from(length).map_err(|_| io::ErrorKind::InvalidInput)?];
        for chunk in bytes.chunks_mut(CHUNK) {
            check_cancelled(cancel)?;
            file.read_exact(chunk)?;
        }
        Ok(bytes)
    }).await
}

pub async fn md5_base64(path: &Path, cancel: &CancelToken) -> io::Result<String> {
    read_source(path, cancel, |path, cancel| {
        #[cfg(test)] super::source_tests::record_read(path, "checksum");
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
    tokio::task::spawn_blocking(move || {
        check_cancelled(&cancel)?;
        let value = read(&path, &cancel)?;
        check_cancelled(&cancel)?;
        Ok(value)
    }).await.map_err(io::Error::other)?
}
