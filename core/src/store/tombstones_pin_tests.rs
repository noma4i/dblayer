use rusqlite::Connection;

use super::tombstones::{prune_tombstones, write_tombstone, TombstoneRetention};

#[test]
fn store_pin_limits_ttl_and_cap_eviction() {
    for ttl_ms in [1, 100] {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch("CREATE TABLE tombstone (model TEXT, id TEXT, seq INTEGER, deleted_at INTEGER, PRIMARY KEY (model, id))").unwrap();
        for (id, revision) in [("old", 4), ("new", 5), ("later", 6)] {
            write_tombstone(&connection, "items", id, revision, 0).unwrap();
        }
        let retention = TombstoneRetention { ttl_ms, min_age_ms: 0, cap: 0, overflow_cap: 0 };
        assert_eq!(prune_tombstones(&connection, 10, &retention, Some(4)).unwrap(), 1);
        let remaining: i64 = connection.query_row("SELECT COUNT(*) FROM tombstone WHERE seq > 4", [], |row| row.get(0)).unwrap();
        assert_eq!(remaining, 2);
        assert_eq!(prune_tombstones(&connection, 10, &retention, None).unwrap(), 2);
    }
}
