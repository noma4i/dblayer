SELECT seq, row_id FROM op_row WHERE seq IN (SELECT value FROM json_each(?1)) ORDER BY seq, pos
