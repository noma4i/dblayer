WITH server_tombstone AS (
    SELECT t.* FROM tombstone t
    WHERE NOT EXISTS (
        SELECT 1 FROM op_effect e JOIN op owner ON owner.seq = e.seq
        WHERE e.model = t.model AND e.row_id = t.id AND e.kind = 'destroy'
          AND owner.status IN ('pending', 'sending', 'failed')
    )
)
SELECT DISTINCT o.seq, t.model, t.id, t.seq FROM op o JOIN op_row r ON r.seq = o.seq JOIN server_tombstone t ON t.id = r.row_id AND t.model = substr(o.action_key, 1, instr(o.action_key, ':') - 1)
UNION
SELECT DISTINCT o.seq, t.model, t.id, t.seq FROM op o JOIN op_transfer x ON x.op_id = o.seq JOIN sync_upload u ON u.transfer_id = x.transfer_id JOIN server_tombstone t ON t.model = u.model AND t.id = u.row_id
