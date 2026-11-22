INSERT INTO cloud_record (domain, model, row_id, version, deleted, pending, system_fields)
VALUES (?1, ?2, ?3, 0, 0, 0, ?4)
ON CONFLICT (domain, model, row_id) DO UPDATE SET system_fields = COALESCE(excluded.system_fields, cloud_record.system_fields)
