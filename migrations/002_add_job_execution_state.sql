ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN IF NOT EXISTS worker_id TEXT,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'jobs_attempts_nonnegative'
    ) THEN
        ALTER TABLE jobs
            ADD CONSTRAINT jobs_attempts_nonnegative CHECK (attempts >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'jobs_max_attempts_positive'
    ) THEN
        ALTER TABLE jobs
            ADD CONSTRAINT jobs_max_attempts_positive CHECK (max_attempts > 0);
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS jobs_status_created_at_idx
    ON jobs (status, created_at);
