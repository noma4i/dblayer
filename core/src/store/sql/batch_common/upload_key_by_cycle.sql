SELECT model || ':' || row_id || ':' || action FROM sync_upload WHERE transfer_id = ?1 AND cycle_id = ?2
