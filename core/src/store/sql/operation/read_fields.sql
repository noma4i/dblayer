SELECT seq, name FROM op_value WHERE seq IN (SELECT value FROM json_each(?1)) AND part = 'input' AND parent = 0 AND name IS NOT NULL AND name != 'id' ORDER BY seq, pos
