INSERT INTO row_alias(model, temp, real) VALUES (?1, ?2, ?3) ON CONFLICT(model, temp) DO UPDATE SET real = excluded.real;
