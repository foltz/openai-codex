CREATE TABLE clear_transitions (
    transition_id TEXT NOT NULL PRIMARY KEY,
    predecessor_thread_id TEXT NOT NULL UNIQUE,
    successor_thread_id TEXT NOT NULL UNIQUE,
    phase TEXT NOT NULL CHECK (
        phase IN (
            'reserved',
            'successor_created',
            'committed',
            'evidence_claimed',
            'completed'
        )
    ),
    end_evidence_state TEXT NOT NULL CHECK (
        end_evidence_state IN ('pending', 'claimed', 'delivered', 'failed')
    ),
    start_evidence_state TEXT NOT NULL CHECK (
        start_evidence_state IN ('pending', 'claimed', 'delivered', 'failed')
    ),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_clear_transitions_phase
    ON clear_transitions(phase, updated_at);
