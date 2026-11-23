UPDATE cloud_record SET confirmed = 1, pending = CASE WHEN version = ?4 AND pending = 1 THEN 0 ELSE pending END, system_fields = CASE WHEN version = ?4 AND pending = 1 THEN COALESCE(?5, system_fields) ELSE COALESCE(system_fields, ?5) END
WHERE domain = ?1 AND model = ?2 AND row_id = ?3
