INSERT INTO cloud_field (domain, model, row_id, path, millis, counter, replica) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
ON CONFLICT(domain, model, row_id, path) DO UPDATE SET millis = excluded.millis, counter = excluded.counter, replica = excluded.replica
