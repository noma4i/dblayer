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
