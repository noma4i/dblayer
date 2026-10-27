use super::journal::{cursor, write_cursor};
use rusqlite::Connection;

#[test]
fn cursor_reads_reject_corrupt_hex_without_shortening() {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute("CREATE TABLE cloud_cursor (id INTEGER PRIMARY KEY, token TEXT NOT NULL)", []).unwrap();
    assert_eq!(cursor(&connection).unwrap(), None);
    write_cursor(&connection, &[0, 127, 255]).unwrap();
    assert_eq!(cursor(&connection).unwrap(), Some(vec![0, 127, 255]));
    for text in ["0", "000", "00ggff", "0g", "\u{00e9}0", "0\u{1f600}"] {
        connection.execute("UPDATE cloud_cursor SET token = ?1", [text]).unwrap();
        assert!(cursor(&connection).is_err(), "{text:?}");
    }
}
