INSERT INTO encrypted_session (id, credential_digest, sequence) VALUES (1, ?1, 0)
         ON CONFLICT(id) DO UPDATE SET credential_digest = excluded.credential_digest, sequence = 0