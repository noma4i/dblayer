CREATE TRIGGER IF NOT EXISTS scope_revision_move AFTER UPDATE OF order_key, place ON scope_member WHEN old.order_key IS NOT new.order_key OR old.place IS NOT new.place BEGIN
  INSERT INTO scope_revision (model, scope_key, row_id, revision)
  VALUES (new.model, new.scope_key, new.row_id, COALESCE((SELECT CAST(value AS INTEGER) FROM manifest WHERE key = 'revision_seq'), 0) + 1)
  ON CONFLICT (model, scope_key, row_id) DO UPDATE SET revision = excluded.revision;
END