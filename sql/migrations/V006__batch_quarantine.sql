-- V006: batch-level quarantine. A batch that fails Bronze validation is moved out of
-- the active Bronze prefix so it can never poison later Silver recomputes.
ALTER TABLE monitoring.ingestion_batches
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'accepted'
        CHECK (status IN ('accepted', 'quarantined')),
    ADD COLUMN IF NOT EXISTS quarantine_reason TEXT;
