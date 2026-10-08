use std::io::Write;

use super::*;

fn file_with(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(bytes).unwrap();
    file
}

#[test]
fn ranges_are_read_without_loading_the_whole_file() {
    let file = file_with(b"0123456789");
    assert_eq!(file_len(file.path()).unwrap(), 10);
    assert_eq!(read_range(file.path(), 3, 4).unwrap(), b"3456");
    assert_eq!(read_range(file.path(), 8, 2).unwrap(), b"89");
    assert!(read_range(file.path(), 8, 5).is_err());
}

#[test]
fn checksum_is_base64_md5() {
    let file = file_with(b"hello");
    assert_eq!(md5_base64(file.path()).unwrap(), "XUFAKrxLKna5cZ2REBfFkg==");
}
