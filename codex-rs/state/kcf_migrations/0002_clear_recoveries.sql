-- Independent caller provenance. No predecessor uniqueness, foreign key,
-- authoritative transition ID, cascading deletion, or automatic expiry.
CREATE TABLE clear_recoveries (
    successor_thread_id TEXT PRIMARY KEY NOT NULL,
    predecessor_thread_id TEXT,
    phase TEXT NOT NULL CHECK (phase IN ('pending', 'complete', 'failed'))
);
