SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'row\_%' ESCAPE '\' AND name != 'row_alias'
         UNION ALL SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name LIKE 'row\_%' ESCAPE '\' AND name != 'row_alias'
