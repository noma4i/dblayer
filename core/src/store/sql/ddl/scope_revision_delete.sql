CREATE TRIGGER IF NOT EXISTS scope_revision_delete AFTER DELETE ON scope_member BEGIN
  INSERT INTO scope_revision (model, scope_key, row_id, revision)
  VALUES (old.model, old.scope_key, old.row_id, COALESCE((SELECT CAST(value AS INTEGER) FROM manifest WHERE key = 'revision_seq'), 0) + 1)
  ON CONFLICT (model, scope_key, row_id) DO UPDATE SET revision = excluded.revision;
END