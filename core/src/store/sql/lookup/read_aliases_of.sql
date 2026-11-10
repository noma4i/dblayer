SELECT a.temp, a.real FROM json_each(?2) AS wanted JOIN row_alias AS a ON a.model = ?1 AND a.real = wanted.value ORDER BY CAST(wanted.key AS INTEGER), a.temp
