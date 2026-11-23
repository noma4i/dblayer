DELETE FROM cloud_progress
WHERE ?1 IS NULL OR domain IN (
    SELECT domain FROM cloud_record WHERE model = ?1 AND deleted = 0
    UNION SELECT domain FROM cloud_field WHERE model = ?1
)
