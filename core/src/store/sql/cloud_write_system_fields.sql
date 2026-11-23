INSERT INTO cloud_record (domain, model, row_id, version, deleted, pending, system_fields, confirmed)
VALUES (?1, ?2, ?3, 0, 0, 0, ?4, 1)
ON CONFLICT (domain, model, row_id) DO UPDATE SET confirmed = 1, system_fields = COALESCE(excluded.system_fields, cloud_record.system_fields)
