CREATE TABLE clear_transitions (
    transition_id TEXT NOT NULL PRIMARY KEY,
    predecessor_thread_id TEXT NOT NULL,
    successor_thread_id TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (
        phase IN (
            'reserved',
            'successor_created',
            'committed',
            'evidence_claimed',
            'completed',
            'abandoned'
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

CREATE UNIQUE INDEX idx_clear_transitions_active_predecessor
    ON clear_transitions(predecessor_thread_id)
    WHERE phase != 'abandoned';

CREATE UNIQUE INDEX idx_clear_transitions_active_successor
    ON clear_transitions(successor_thread_id)
    WHERE phase != 'abandoned';
