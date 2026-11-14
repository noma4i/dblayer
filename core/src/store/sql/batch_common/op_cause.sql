UPDATE op SET cause_kind = ?2, cause_limit = ?3 WHERE seq = ?1 AND (cause_kind IS NOT ?2 OR cause_limit IS NOT ?3)
