SELECT model, row_id, version, deleted FROM cloud_record
WHERE domain = ?1 AND pending = 1
ORDER BY version, model, row_id LIMIT ?2
