DELETE FROM cloud_field
WHERE (?1 IS NULL OR model = ?1) AND NOT EXISTS (
    SELECT 1 FROM cloud_record
    WHERE cloud_record.domain = cloud_field.domain AND cloud_record.model = cloud_field.model
      AND cloud_record.row_id = cloud_field.row_id AND cloud_record.deleted = 1
)
