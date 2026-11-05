use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const PART_SUFFIX: &str = ".part";
const COPY_CHUNK: usize = 1024 * 1024;

pub(crate) enum CopiedSource {
    Ready { size: u64, sha256: String, data: Vec<u8>, location: super::Location },
    Unreadable(String),
}

pub(crate) struct FileDirectories {
    directory: Option<PathBuf>,
    cache: PathBuf,
    scratch: bool,
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

fn ignore_missing(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

#[cfg(test)]
fn names(directory: &Path) -> io::Result<Vec<String>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut names = entries.map(|entry| {
        entry?.file_name().into_string().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid file name"))
    }).collect::<io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

impl FileDirectories {
    pub(crate) fn beside(database: &Path) -> Self {
        Self { directory: Some(suffixed(database, ".files")), cache: suffixed(database, ".cache"), scratch: false }
    }

    pub(crate) fn remove_all(&self) -> io::Result<()> {
        if let Some(directory) = &self.directory {
            ignore_missing(fs::remove_dir_all(directory))?;
        }
        ignore_missing(fs::remove_dir_all(&self.cache))
    }

    pub(crate) fn scratch() -> Self {
        let cache = std::env::temp_dir().join(format!("dblayer-{}", uuid::Uuid::new_v4()));
        Self { directory: None, cache, scratch: true }
    }

    pub(crate) fn has_directory(&self) -> bool {
        self.directory.is_some()
    }

    pub(crate) fn path(&self, id: &str) -> Option<PathBuf> {
        self.directory.as_ref().map(|directory| directory.join(id))
    }

    pub(crate) fn write(&self, id: &str, bytes: &[u8]) -> io::Result<PathBuf> {
        let target = self.path(id).ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "no file directory"))?;
        write_atomic(&target, &suffixed(&target, PART_SUFFIX), bytes)?;
        Ok(target)
    }

    pub(crate) fn copy_source(&self, id: &str, source: &Path) -> io::Result<CopiedSource> {
        let result = self.copy_into(id, source);
        if !matches!(&result, Ok(CopiedSource::Ready { .. })) {
            self.discard(&[id.to_string()])?;
        }
        result
    }

    fn copy_into(&self, id: &str, source: &Path) -> io::Result<CopiedSource> {
        use sha2::{Digest, Sha256};
        let mut source = match File::open(source) {
            Ok(source) => source,
            Err(error) => return Ok(CopiedSource::Unreadable(error.to_string())),
        };
        let mut buffer = vec![0; COPY_CHUNK];
        let mut data = Vec::new();
        let mut disk: Option<(File, PathBuf)> = None;
        let mut size = 0u64;
        let mut digest = Sha256::new();
        loop {
            let length = match source.read(&mut buffer) {
                Ok(0) => break,
                Ok(length) => length,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Ok(CopiedSource::Unreadable(error.to_string())),
            };
            let chunk = &buffer[..length];
            size += length as u64;
            digest.update(chunk);
            if disk.is_none() && size > super::TABLE_LIMIT as u64 {
                if let Some(target) = self.path(id) {
                    if let Some(parent) = target.parent() { fs::create_dir_all(parent)?; }
                    let mut file = File::create(suffixed(&target, PART_SUFFIX))?;
                    file.write_all(&data)?;
                    data = Vec::new();
                    disk = Some((file, target));
                }
            }
            match &mut disk {
                Some((file, _)) => file.write_all(chunk)?,
                None => data.extend_from_slice(chunk),
            }
        }
        let location = if let Some((file, target)) = disk {
            file.sync_all()?;
            drop(file);
            fs::rename(suffixed(&target, PART_SUFFIX), target)?;
            super::Location::Directory
        } else { super::Location::Table };
        Ok(CopiedSource::Ready { size, sha256: digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect(), data, location })
    }

    pub(crate) fn read(&self, id: &str) -> io::Result<Vec<u8>> {
        let path = self.path(id).ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "no file directory"))?;
        fs::read(path)
    }

    pub(crate) fn cache_target(&self, id: &str, name: &str) -> PathBuf {
        let leaf = Path::new(name).file_name().and_then(|leaf| leaf.to_str()).filter(|leaf| !leaf.is_empty()).unwrap_or("file");
        self.cache.join(id).join(leaf)
    }

    pub(crate) fn materialize(&self, id: &str, name: &str, bytes: &[u8]) -> io::Result<PathBuf> {
        #[cfg(test)] super::directories_tests::record_io(id, "materialize");
        let target = self.cache_target(id, name);
        if !target.exists() {
            static WRITERS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let writer = WRITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            write_atomic(&target, &suffixed(&target, &format!(".{writer}{PART_SUFFIX}")), bytes)?;
        }
        Ok(target)
    }

    pub(crate) fn discard(&self, ids: &[String]) -> io::Result<()> {
        for id in ids {
            #[cfg(test)] super::directories_tests::record_io(id, "discard");
            if let Some(path) = self.path(id) {
                ignore_missing(fs::remove_file(suffixed(&path, PART_SUFFIX)))?;
                ignore_missing(fs::remove_file(path))?;
            }
            ignore_missing(fs::remove_dir_all(self.cache.join(id)))?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn directory_entries(&self) -> io::Result<Vec<String>> {
        self.directory.as_deref().map(names).unwrap_or_else(|| Ok(Vec::new()))
    }

    #[cfg(test)]
    pub(crate) fn cache_entries(&self) -> io::Result<Vec<String>> {
        names(&self.cache)
    }


}


fn write_atomic(target: &Path, partial: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(partial)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(partial, target)
}

impl Drop for FileDirectories {
    fn drop(&mut self) {
        if self.scratch {
            let _ = fs::remove_dir_all(&self.cache);
        }
    }
}
