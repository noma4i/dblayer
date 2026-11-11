INSERT INTO transfer_outcome (transfer_id, cycle_id, kind, revision, value, terminal_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(transfer_id, cycle_id) DO NOTHING
