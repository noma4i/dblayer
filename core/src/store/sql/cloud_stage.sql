INSERT INTO cloud_record (domain, model, row_id, version, deleted, pending)
VALUES (?1, ?2, ?3, ?4, ?5, 1)
ON CONFLICT (domain, model, row_id) DO UPDATE SET
    version = excluded.version, deleted = excluded.deleted, pending = 1
WHERE excluded.version > cloud_record.version
