use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use base64::Engine;
use md5::{Digest, Md5};

const CHUNK: usize = 1024 * 1024;

pub fn file_len(path: &Path) -> io::Result<u64> {
    Ok(std::fs::metadata(path)?.len())
}

pub fn read_range(path: &Path, offset: u64, length: u64) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0; usize::try_from(length).map_err(|_| io::ErrorKind::InvalidInput)?];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

pub fn md5_base64(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Md5::new();
    let mut buffer = vec![0; CHUNK];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(hasher.finalize()))
}
