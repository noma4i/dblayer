CREATE TRIGGER IF NOT EXISTS scope_edge_member_delete AFTER DELETE ON scope_member BEGIN
  DELETE FROM scope_edge WHERE model = old.model AND scope_key = old.scope_key AND row_id = old.row_id;
END