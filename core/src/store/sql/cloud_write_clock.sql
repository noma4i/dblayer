INSERT INTO cloud_clock (domain, millis, counter, replica) VALUES (?1, ?2, ?3, ?4)
ON CONFLICT(domain) DO UPDATE SET millis = excluded.millis, counter = excluded.counter, replica = excluded.replica
