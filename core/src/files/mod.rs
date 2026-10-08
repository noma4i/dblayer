mod descriptor;
mod directories;
mod table;

pub use descriptor::FileDescriptor;
pub use table::{FileRecord, Location};

pub(crate) use descriptor::sha256_hex;
pub(crate) use directories::{is_partial, remove_beside, FileDirectories};
pub(crate) use table::{
    all_references, clear_all, clear_model, find, ids_of_rows, move_row, put, remove_pair, remove_row, FileReference,
};

pub(crate) const TABLE_LIMIT: usize = 256 * 1024;

pub(crate) fn placement(length: usize, has_directory: bool) -> Location {
    if length <= TABLE_LIMIT || !has_directory {
        Location::Table
    } else {
        Location::Directory
    }
}

#[cfg(test)]
mod descriptor_tests;
#[cfg(test)]
mod directories_tests;
#[cfg(test)]
mod table_tests;
