use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const PART_SUFFIX: &str = ".part";

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
        write_atomic(&target, bytes)?;
        Ok(target)
    }

    pub(crate) fn read(&self, id: &str) -> io::Result<Vec<u8>> {
        let path = self.path(id).ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "no file directory"))?;
        fs::read(path)
    }

    pub(crate) fn materialize(&self, id: &str, name: &str, bytes: &[u8]) -> io::Result<PathBuf> {
        let leaf = Path::new(name).file_name().and_then(|leaf| leaf.to_str()).filter(|leaf| !leaf.is_empty()).unwrap_or("file");
        let target = self.cache.join(id).join(leaf);
        if !target.exists() {
            write_atomic(&target, bytes)?;
        }
        Ok(target)
    }

    pub(crate) fn discard(&self, ids: &[String]) -> io::Result<()> {
        for id in ids {
            if let Some(path) = self.path(id) {
                ignore_missing(fs::remove_file(path))?;
            }
            ignore_missing(fs::remove_dir_all(self.cache.join(id)))?;
        }
        Ok(())
    }

    pub(crate) fn directory_entries(&self) -> io::Result<Vec<String>> {
        self.directory.as_deref().map(names).unwrap_or_else(|| Ok(Vec::new()))
    }

    pub(crate) fn cache_entries(&self) -> io::Result<Vec<String>> {
        names(&self.cache)
    }

    pub(crate) fn remove_directory_entry(&self, name: &str) -> io::Result<()> {
        if let Some(directory) = &self.directory {
            ignore_missing(fs::remove_file(directory.join(name)))?;
        }
        Ok(())
    }

    pub(crate) fn remove_cache_entry(&self, name: &str) -> io::Result<()> {
        ignore_missing(fs::remove_dir_all(self.cache.join(name)))
    }
}

pub(crate) fn is_partial(name: &str) -> bool {
    name.ends_with(PART_SUFFIX)
}

fn write_atomic(target: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let partial = suffixed(target, PART_SUFFIX);
    let mut file = File::create(&partial)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&partial, target)
}

impl Drop for FileDirectories {
    fn drop(&mut self) {
        if self.scratch {
            let _ = fs::remove_dir_all(&self.cache);
        }
    }
}
