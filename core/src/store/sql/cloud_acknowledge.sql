UPDATE cloud_record SET pending = 0, system_fields = ?5
WHERE domain = ?1 AND model = ?2 AND row_id = ?3 AND version = ?4 AND pending = 1
