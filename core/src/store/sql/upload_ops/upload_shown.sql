UPDATE sync_upload SET status = COALESCE(?4, status), stage = COALESCE(?5, stage), wait = ?6 WHERE model = ?1 AND row_id = ?2 AND action = ?3 AND ?4 IS NOT NULL
