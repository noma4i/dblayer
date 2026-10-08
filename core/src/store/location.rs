use std::fs;
use std::path::{Path, PathBuf};

use crate::files::remove_beside;

use super::StoreError;

pub const DIRECTORY_NAME: &str = "DBlayer";
pub const FILE_EXTENSION: &str = "sqlite";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreLocation {
    base: PathBuf,
}

impl StoreLocation {
    pub fn new(base: &Path) -> Self {
        Self { base: base.to_path_buf() }
    }

    pub fn directory(&self) -> PathBuf {
        self.base.join(DIRECTORY_NAME)
    }

    pub fn database_path(&self, user_id: &str) -> Result<PathBuf, StoreError> {
        let valid = !user_id.is_empty() && !user_id.contains(['/', '\\', '\0']) && user_id != "." && user_id != "..";
        if !valid {
            return Err(StoreError::Constraint(format!("user id {user_id:?} is not a file name")));
        }
        Ok(self.directory().join(format!("{user_id}.{FILE_EXTENSION}")))
    }

    pub fn prepare_directory(&self) -> Result<PathBuf, StoreError> {
        let directory = self.directory();
        fs::create_dir_all(&directory)?;
        Ok(directory)
    }
}

pub(crate) fn reset_marker(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".reset");
    PathBuf::from(name)
}

pub(crate) fn database_files(path: &Path) -> [PathBuf; 3] {
    let suffixed = |suffix: &str| {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        PathBuf::from(name)
    };
    [path.to_path_buf(), suffixed("-wal"), suffixed("-shm")]
}

pub(crate) fn remove_database_files(path: &Path) -> Result<(), StoreError> {
    for file in database_files(path) {
        match fs::remove_file(&file) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(remove_beside(path)?)
}
