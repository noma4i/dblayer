CREATE UNIQUE INDEX IF NOT EXISTS op_open_dedupe ON op (action_key, dedupe_key)
  WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'sending')