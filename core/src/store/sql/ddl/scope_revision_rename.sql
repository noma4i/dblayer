CREATE TRIGGER IF NOT EXISTS scope_revision_rename AFTER UPDATE OF model, scope_key, row_id ON scope_member WHEN old.model IS NOT new.model OR old.scope_key IS NOT new.scope_key OR old.row_id IS NOT new.row_id BEGIN
  INSERT INTO scope_revision (model, scope_key, row_id, revision)
  VALUES (old.model, old.scope_key, old.row_id, COALESCE((SELECT CAST(value AS INTEGER) FROM manifest WHERE key = 'revision_seq'), 0) + 1)
  ON CONFLICT (model, scope_key, row_id) DO UPDATE SET revision = excluded.revision;
  INSERT INTO scope_revision (model, scope_key, row_id, revision)
  VALUES (new.model, new.scope_key, new.row_id, COALESCE((SELECT CAST(value AS INTEGER) FROM manifest WHERE key = 'revision_seq'), 0) + 1)
  ON CONFLICT (model, scope_key, row_id) DO UPDATE SET revision = excluded.revision;
END