SELECT DISTINCT e.seq FROM op_effect e JOIN op ON op.seq = e.seq WHERE e.model = ?1 AND e.row_id IN (SELECT value FROM json_each(?2)) AND e.kind = 'edit' AND op.status IN {}
