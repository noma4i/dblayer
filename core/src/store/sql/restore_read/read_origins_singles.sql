SELECT op.seq, upload.cycle_id, upload.row_id FROM op LEFT JOIN sync_upload AS upload ON upload.transfer_id = op.transfer_id WHERE op.intent_state = 'ready' AND op.origin_kind = 'transfer'
