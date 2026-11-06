CREATE TRIGGER IF NOT EXISTS scope_edge_member_rename AFTER UPDATE OF row_id ON scope_member BEGIN
  UPDATE OR REPLACE scope_edge SET row_id = new.row_id WHERE model = old.model AND scope_key = old.scope_key AND row_id = old.row_id;
END