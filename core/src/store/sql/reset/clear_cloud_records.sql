DELETE FROM cloud_record WHERE deleted = 0 AND (?1 IS NULL OR model = ?1)
