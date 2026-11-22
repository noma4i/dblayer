CREATE TABLE IF NOT EXISTS cloud_record (
    domain TEXT NOT NULL,
    model TEXT NOT NULL,
    row_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    deleted INTEGER NOT NULL,
    pending INTEGER NOT NULL,
    PRIMARY KEY (domain, model, row_id)
);
CREATE TABLE IF NOT EXISTS cloud_progress (
    domain TEXT PRIMARY KEY NOT NULL,
    bytes BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS cloud_record_pending ON cloud_record(domain, version, model, row_id) WHERE pending = 1;
CREATE TABLE IF NOT EXISTS cloud_clock (
    domain TEXT PRIMARY KEY NOT NULL,
    millis BLOB NOT NULL CHECK(length(millis) = 8),
    counter INTEGER NOT NULL CHECK(counter BETWEEN 0 AND 4294967295),
    replica BLOB NOT NULL CHECK(length(replica) = 16)
);
CREATE TABLE IF NOT EXISTS cloud_field (
    domain TEXT NOT NULL,
    model TEXT NOT NULL,
    row_id TEXT NOT NULL,
    path TEXT NOT NULL,
    millis BLOB NOT NULL CHECK(length(millis) = 8),
    counter INTEGER NOT NULL CHECK(counter BETWEEN 0 AND 4294967295),
    replica BLOB NOT NULL CHECK(length(replica) = 16),
    PRIMARY KEY (domain, model, row_id, path)
);
