INSERT INTO cloud_progress (domain, bytes) VALUES (?1, ?2)
ON CONFLICT (domain) DO UPDATE SET bytes = excluded.bytes
