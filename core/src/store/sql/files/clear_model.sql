DELETE FROM file
WHERE {scope}
  AND NOT EXISTS (
    SELECT 1 FROM sync_upload
    WHERE sync_upload.model = file.model AND sync_upload.row_id = file.row_id
      AND file.id = 'transfers/' || sync_upload.transfer_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM op_value AS leaf
    JOIN op_value AS entry ON entry.seq = leaf.seq AND entry.part = leaf.part AND entry.node = leaf.parent
    JOIN op_value AS files ON files.seq = entry.seq AND files.part = entry.part AND files.node = entry.parent
    WHERE leaf.part = 'prior' AND leaf.name = 'id' AND leaf.text = file.id
      AND entry.kind = 5 AND files.name = 'files' AND files.kind = 6
  )
