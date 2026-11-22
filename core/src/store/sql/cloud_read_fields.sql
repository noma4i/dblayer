SELECT millis, counter, replica, path FROM cloud_field WHERE domain = ?1 AND model = ?2 AND row_id = ?3 ORDER BY path
