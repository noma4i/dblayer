use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const PART_SUFFIX: &str = ".part";
const COPY_CHUNK: usize = 1024 * 1024;

/// The text of the refusal of an empty source: at the call and at the copy of a file that was emptied after the call read its size.
pub(crate) const EMPTY_SOURCE: &str = "the source file is empty";

static COPY_ADMISSION: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(crate) enum CopiedSource {
    Ready { size: u64, sha256: String, data: Vec<u8>, location: super::Location },
    Unreadable(String),
}

pub(crate) struct FileDirectories {
    directory: Option<PathBuf>,
    cache: PathBuf,
    scratch: bool,
    #[cfg(test)]
    source_reads: std::sync::atomic::AtomicU64,
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
        let directory = suffixed(database, ".files");
        Self { cache: directory.join("cache"), directory: Some(directory), scratch: false, #[cfg(test)] source_reads: Default::default() }
    }

    pub(crate) fn remove_all(&self) -> io::Result<()> {
        if let Some(directory) = &self.directory {
            ignore_missing(fs::remove_dir_all(directory))?;
        }
        ignore_missing(fs::remove_dir_all(&self.cache))
    }

    pub(crate) fn scratch() -> Self {
        let directory = std::env::temp_dir().join(format!("dblayer-{}", uuid::Uuid::new_v4()));
        Self { cache: directory.join("cache"), directory: Some(directory), scratch: true, #[cfg(test)] source_reads: Default::default() }
    }

    /// Removes the directory and the cache of a scratch (in-memory) database; an on-disk database keeps its files.
    pub(crate) fn remove_scratch(&self) {
        if !self.scratch { return; }
        if let Some(directory) = &self.directory { let _ = fs::remove_dir_all(directory); }
        let _ = fs::remove_dir_all(&self.cache);
    }

    pub(crate) fn has_directory(&self) -> bool {
        self.directory.is_some()
    }

    pub(crate) fn path(&self, id: &str) -> Option<PathBuf> {
        self.directory.as_ref().map(|directory| directory.join(id))
    }

    pub(crate) fn prepare(&self, id: &str, bytes: &[u8]) -> io::Result<()> {
        let target = self.path(id).ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "no file directory"))?;
        if let Some(parent) = target.parent() { fs::create_dir_all(parent)?; }
        let partial = suffixed(&target, ".partial");
        let mut file = fs::OpenOptions::new().write(true).create_new(true).open(&partial)?;
        let result = (|| {
            file.write_all(bytes)?;
            file.sync_all()
        })();
        drop(file);
        if result.is_err() { fs::remove_file(partial)?; }
        result
    }

    pub(crate) fn finish(&self, id: &str) -> io::Result<()> {
        let target = self.path(id).ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "no file directory"))?;
        fs::rename(suffixed(&target, ".partial"), target)
    }

    pub(crate) fn discard_partial(&self, id: &str) -> io::Result<()> {
        let Some(target) = self.path(id) else { return Ok(()) };
        ignore_missing(fs::remove_file(suffixed(&target, ".partial")))
    }

    pub(crate) fn recover_partials(&self, ids: &std::collections::HashSet<String>) -> io::Result<()> {
        let Some(directory) = &self.directory else { return Ok(()) };
        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_file() { continue; }
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(id) = name.strip_suffix(".partial") {
                let target = directory.join(id);
                if ids.contains(id) && !target.exists() { fs::rename(entry.path(), target)?; } else { fs::remove_file(entry.path())?; }
            } else if !ids.contains(name) {
                self.discard(&[name.to_string()])?;
            }
        }
        match fs::read_dir(&self.cache) {
            Ok(entries) => for entry in entries {
                let entry = entry?;
                if !entry.file_type()?.is_file() { continue; }
                if let Some(name) = entry.file_name().to_str() {
                    if !ids.contains(name) { fs::remove_file(entry.path())?; }
                }
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Ok(())
    }

    pub(crate) fn write(&self, id: &str, bytes: &[u8]) -> io::Result<PathBuf> {
        let target = self.path(id).ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "no file directory"))?;
        write_atomic(&target, &suffixed(&target, PART_SUFFIX), bytes)?;
        Ok(target)
    }

    pub(crate) fn copy_source(&self, id: &str, source: &Path) -> io::Result<CopiedSource> {
        match self.copy_admitted(id, source, false)? {
            CopiedSource::Ready { size: 0, .. } => Ok(CopiedSource::Unreadable(EMPTY_SOURCE.into())),
            copied => Ok(copied),
        }
    }

    /// The copy of an attached file: a large file stays in `<id>.partial` until the batch that names it commits (`finish`) or fails (`discard_partial`).
    pub(crate) fn copy_source_partial(&self, id: &str, source: &Path) -> io::Result<CopiedSource> {
        self.copy_admitted(id, source, true)
    }

    fn copy_admitted(&self, id: &str, source: &Path, keep_partial: bool) -> io::Result<CopiedSource> {
        let _admitted = COPY_ADMISSION.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(test)] self.source_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let result = self.copy_into(id, source, keep_partial);
        if !matches!(&result, Ok(CopiedSource::Ready { .. })) {
            self.discard(&[id.to_string()])?;
            self.discard_partial(id)?;
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn source_reads(&self) -> u64 { self.source_reads.load(std::sync::atomic::Ordering::Relaxed) }

    #[cfg(test)]
    pub(crate) fn record_source_read(&self) { self.source_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }

    fn copy_into(&self, id: &str, source: &Path, keep_partial: bool) -> io::Result<CopiedSource> {
        use sha2::{Digest, Sha256};
        let mut source = match File::open(source) {
            Ok(source) => source,
            Err(error) => return Ok(CopiedSource::Unreadable(error.to_string())),
        };
        let suffix = if keep_partial { ".partial" } else { PART_SUFFIX };
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
                    let mut file = File::create(suffixed(&target, suffix))?;
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
            if !keep_partial { fs::rename(suffixed(&target, suffix), target)?; }
            super::Location::Directory
        } else { super::Location::Table };
        Ok(CopiedSource::Ready { size, sha256: digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect(), data, location })
    }

    pub(crate) fn cache_target(&self, id: &str, _name: &str) -> PathBuf {
        self.cache.join(id)
    }

    pub(crate) fn materialize(&self, id: &str, name: &str, bytes: &[u8]) -> io::Result<PathBuf> {
        #[cfg(test)] super::tests::directories_tests::record_io(id, "materialize");
        let target = self.cache_target(id, name);
        if !target.exists() {
            static WRITERS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let writer = WRITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            write_atomic(&target, &suffixed(&target, &format!(".{writer}{PART_SUFFIX}")), bytes)?;
        }
        Ok(target)
    }

    /// The file `from` takes the place of the file `to` in one step, in the directory and in the cache; a missing `from` changes nothing.
    pub(crate) fn promote(&self, from: &str, to: &str) -> io::Result<()> {
        if let (Some(from), Some(to)) = (self.path(from), self.path(to)) { swap_file(&from, &to)?; }
        swap_file(&self.cache.join(from), &self.cache.join(to))
    }

    pub(crate) fn keep_prepared(&self, from: &str, file: &str) -> io::Result<()> {
        let target = self.path(&format!("prepared/{file}")).ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "no file directory"))?;
        if let Some(parent) = target.parent() { fs::create_dir_all(parent)?; }
        let source = self.path(from).ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "no file directory"))?;
        fs::rename(source, target)
    }

    pub(crate) fn discard(&self, ids: &[String]) -> io::Result<()> {
        for id in ids {
            #[cfg(test)] super::tests::directories_tests::record_io(id, "discard");
            if let Some(path) = self.path(id) {
                ignore_missing(fs::remove_file(suffixed(&path, PART_SUFFIX)))?;
                ignore_missing(fs::remove_file(path))?;
            }
            ignore_missing(fs::remove_file(self.cache.join(id)))?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn directory_entries(&self) -> io::Result<Vec<String>> {
        self.directory.as_deref().map(names).unwrap_or_else(|| Ok(Vec::new())).map(|entries| entries.into_iter().filter(|entry| entry != "cache").collect())
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
        self.remove_scratch();
    }
}

fn swap_file(from: &Path, to: &Path) -> io::Result<()> { ignore_missing(fs::rename(from, to)) }
