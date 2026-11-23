CREATE TABLE IF NOT EXISTS cloud_record (
    domain TEXT NOT NULL,
    model TEXT NOT NULL,
    row_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    deleted INTEGER NOT NULL,
    pending INTEGER NOT NULL,
    system_fields BLOB,
    confirmed INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (domain, model, row_id)
);
CREATE TABLE IF NOT EXISTS cloud_waiting (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    digest BLOB NOT NULL,
    model TEXT NOT NULL,
    row_id TEXT NOT NULL,
    server_revision INTEGER,
    deleted INTEGER NOT NULL,
    system_fields BLOB,
    UNIQUE (domain, digest)
);
CREATE TABLE IF NOT EXISTS cloud_waiting_dependency (
    domain TEXT NOT NULL,
    model TEXT NOT NULL,
    row_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    PRIMARY KEY (domain, model, row_id, seq)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS cloud_waiting_dependency_seq ON cloud_waiting_dependency(domain, seq);
CREATE TABLE IF NOT EXISTS cloud_waiting_ready (
    domain TEXT NOT NULL,
    seq INTEGER NOT NULL,
    PRIMARY KEY (domain, seq)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS cloud_waiting_field (
    seq INTEGER NOT NULL,
    field INTEGER NOT NULL,
    millis BLOB NOT NULL CHECK(length(millis) = 8),
    counter INTEGER NOT NULL,
    replica BLOB NOT NULL CHECK(length(replica) = 16),
    PRIMARY KEY (seq, field)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS cloud_waiting_path (
    seq INTEGER NOT NULL,
    field INTEGER NOT NULL,
    position INTEGER NOT NULL,
    segment TEXT NOT NULL,
    PRIMARY KEY (seq, field, position)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS cloud_progress (
    domain TEXT PRIMARY KEY NOT NULL,
    bytes BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS cloud_bootstrap (
    domain TEXT PRIMARY KEY NOT NULL,
    model TEXT NOT NULL,
    row_id TEXT NOT NULL,
    done INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS cloud_record_identity ON cloud_record(model, row_id, domain);
CREATE INDEX IF NOT EXISTS cloud_record_pending ON cloud_record(domain, version, model, row_id) WHERE pending = 1;
CREATE INDEX IF NOT EXISTS cloud_record_issues ON cloud_record(domain, model, row_id) WHERE pending IN (2, 3);
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
