DELETE FROM cloud_field WHERE domain = ?1 AND model = ?2 AND row_id = ?3 AND (substr(path, 1, length(?4)) = ?4 OR substr(?4, 1, length(path)) = path)
