SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'row\_%' ESCAPE '\'
         UNION ALL SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name LIKE 'row\_%' ESCAPE '\'