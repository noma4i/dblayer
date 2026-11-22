SELECT model, row_id, version, deleted, system_fields FROM cloud_record
WHERE domain = ?1 AND pending = 1 AND NOT EXISTS ({OPEN_OPERATIONS})
ORDER BY version, model, row_id LIMIT ?2
