SELECT seq FROM op
WHERE seq IN (
    SELECT seq FROM op_row WHERE row_id = {row_id}
    UNION
    SELECT seq FROM op WHERE temp_id = {row_id}
)
AND substr(action_key, 1, instr(action_key, ':') - 1) = {model}
AND status IN {OPEN_STATUSES}
