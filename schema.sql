-- PostgreSQL schema for the DAG scheduler, execution lifecycle, messaging,
-- and audit records described in 消息与log改造.md.
--
-- This file intentionally excludes the legacy Message Hub message table.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Current DAG scheduler schema, extended for execution and Agent Card tracking.
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    subject TEXT NOT NULL,
    description TEXT,
    thread_id TEXT,
    owner TEXT,
    status TEXT,
    blocked_by_count INT NOT NULL DEFAULT 0,
    claimed_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ,
    metadata JSONB,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    task_type TEXT NOT NULL DEFAULT 'general',
    work_shard INT NOT NULL DEFAULT 0,
    card_selector JSONB,
    current_execution_id UUID,
    current_attempt INT
);

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS thread_id TEXT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS owner TEXT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS last_heartbeat TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS metadata JSONB;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS task_type TEXT NOT NULL DEFAULT 'general';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS work_shard INT NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS card_selector JSONB;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS current_execution_id UUID;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS current_attempt INT;

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check
    CHECK (status IN ('pending', 'in_progress', 'completed', 'failed', 'cancelled'));

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_current_attempt_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_current_attempt_check
    CHECK (current_attempt IS NULL OR current_attempt >= 1);

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_work_shard_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_work_shard_check
    CHECK (work_shard >= 0);

CREATE INDEX IF NOT EXISTS idx_tasks_ready
    ON tasks (status, blocked_by_count, updated_at)
    WHERE status = 'pending' AND blocked_by_count = 0;

CREATE INDEX IF NOT EXISTS idx_tasks_lease
    ON tasks (status, lease_expires_at)
    WHERE status = 'in_progress';

CREATE INDEX IF NOT EXISTS idx_tasks_thread
    ON tasks (thread_id, status, blocked_by_count, updated_at)
    WHERE status = 'pending' AND blocked_by_count = 0;

CREATE INDEX IF NOT EXISTS idx_tasks_ready_shard
    ON tasks (thread_id, task_type, work_shard, updated_at, id)
    WHERE status = 'pending' AND blocked_by_count = 0;

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    blocker_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, blocker_id)
);

CREATE INDEX IF NOT EXISTS idx_deps_blocker
    ON task_dependencies (blocker_id);

-- Versioned Agent Card registry.
CREATE TABLE IF NOT EXISTS agent_cards (
    agent_card_id TEXT NOT NULL,
    version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'deprecated', 'revoked')),
    task_types JSONB NOT NULL,
    system_prompt TEXT NOT NULL,
    tool_allowlist JSONB NOT NULL DEFAULT '[]'::jsonb,
    skill_allowlist JSONB NOT NULL DEFAULT '[]'::jsonb,
    runtime_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    bundle_ref TEXT NOT NULL,
    bundle_digest TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agent_card_id, version)
);

CREATE INDEX IF NOT EXISTS idx_agent_cards_active
    ON agent_cards (status)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_agent_cards_task_types
    ON agent_cards USING GIN (task_types)
    WHERE status = 'active';

CREATE OR REPLACE FUNCTION reject_agent_card_content_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.agent_card_id IS DISTINCT FROM OLD.agent_card_id
       OR NEW.version IS DISTINCT FROM OLD.version
       OR NEW.task_types IS DISTINCT FROM OLD.task_types
       OR NEW.system_prompt IS DISTINCT FROM OLD.system_prompt
       OR NEW.tool_allowlist IS DISTINCT FROM OLD.tool_allowlist
       OR NEW.skill_allowlist IS DISTINCT FROM OLD.skill_allowlist
       OR NEW.runtime_config IS DISTINCT FROM OLD.runtime_config
       OR NEW.bundle_ref IS DISTINCT FROM OLD.bundle_ref
       OR NEW.bundle_digest IS DISTINCT FROM OLD.bundle_digest
       OR NEW.config_hash IS DISTINCT FROM OLD.config_hash
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION
            'Agent Card % version % is immutable; publish a new version instead',
            OLD.agent_card_id, OLD.version;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS agent_cards_immutable_content ON agent_cards;
CREATE TRIGGER agent_cards_immutable_content
BEFORE UPDATE ON agent_cards
FOR EACH ROW
EXECUTE FUNCTION reject_agent_card_content_update();

-- Runtime registration and lease state.
CREATE TABLE IF NOT EXISTS agent_runtimes (
    runtime_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (
        status IN ('starting', 'idle', 'loading', 'running', 'unloading', 'stopped')
    ),
    runtime_version TEXT NOT NULL,
    current_execution_id UUID,
    current_attempt INT,
    last_heartbeat TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE agent_runtimes DROP CONSTRAINT IF EXISTS agent_runtimes_current_attempt_check;
ALTER TABLE agent_runtimes ADD CONSTRAINT agent_runtimes_current_attempt_check
    CHECK (current_attempt IS NULL OR current_attempt >= 1);

ALTER TABLE agent_runtimes DROP CONSTRAINT IF EXISTS agent_runtimes_current_execution_check;
ALTER TABLE agent_runtimes ADD CONSTRAINT agent_runtimes_current_execution_check
    CHECK (
        (current_execution_id IS NULL AND current_attempt IS NULL)
        OR (current_execution_id IS NOT NULL AND current_attempt IS NOT NULL)
    );

CREATE INDEX IF NOT EXISTS idx_agent_runtimes_available
    ON agent_runtimes (status, lease_expires_at)
    WHERE status = 'idle';

-- Each retry creates a new attempt under the same execution_id.
CREATE TABLE IF NOT EXISTS task_executions (
    execution_id UUID NOT NULL,
    attempt INT NOT NULL CHECK (attempt >= 1),
    task_id TEXT NOT NULL REFERENCES tasks(id),
    thread_id TEXT,
    runtime_id TEXT REFERENCES agent_runtimes(runtime_id),
    agent_card_id TEXT NOT NULL,
    agent_card_version TEXT NOT NULL,
    agent_card_digest TEXT NOT NULL,
    agent_card_config_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('claimed', 'running', 'completed', 'failed', 'expired', 'cancelled')
    ),
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    result_summary TEXT,
    result_path TEXT,
    failure_reason TEXT,
    PRIMARY KEY (execution_id, attempt),
    UNIQUE (task_id, execution_id, attempt),
    FOREIGN KEY (agent_card_id, agent_card_version)
        REFERENCES agent_cards (agent_card_id, version)
);

CREATE INDEX IF NOT EXISTS idx_task_executions_task
    ON task_executions (task_id, claimed_at DESC);

CREATE INDEX IF NOT EXISTS idx_task_executions_active_lease
    ON task_executions (lease_expires_at)
    WHERE status IN ('claimed', 'running');

CREATE INDEX IF NOT EXISTS idx_task_executions_runtime
    ON task_executions (runtime_id, status, lease_expires_at)
    WHERE status IN ('claimed', 'running');

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_current_execution_fkey;
ALTER TABLE tasks ADD CONSTRAINT tasks_current_execution_fkey
    FOREIGN KEY (id, current_execution_id, current_attempt)
    REFERENCES task_executions (task_id, execution_id, attempt)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE agent_runtimes DROP CONSTRAINT IF EXISTS agent_runtimes_current_execution_fkey;
ALTER TABLE agent_runtimes ADD CONSTRAINT agent_runtimes_current_execution_fkey
    FOREIGN KEY (current_execution_id, current_attempt)
    REFERENCES task_executions (execution_id, attempt)
    DEFERRABLE INITIALLY DEFERRED;

-- Transactional outbox for publishing PostgreSQL state changes to RocketMQ.
CREATE TABLE IF NOT EXISTS message_outbox (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic TEXT NOT NULL,
    tag TEXT NOT NULL,
    message_key TEXT,
    envelope JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'published', 'failed')),
    retry_count INT NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    next_retry_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_message_outbox_pending
    ON message_outbox (next_retry_at, created_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_message_outbox_key
    ON message_outbox (message_key, created_at DESC);

-- Per-consumer idempotency record for at-least-once message delivery.
CREATE TABLE IF NOT EXISTS message_consumer_inbox (
    consumer_name TEXT NOT NULL,
    event_id UUID NOT NULL,
    consumed_at TIMESTAMPTZ,
    result TEXT,
    PRIMARY KEY (consumer_name, event_id)
);

ALTER TABLE message_consumer_inbox
    ALTER COLUMN consumed_at DROP NOT NULL;
ALTER TABLE message_consumer_inbox
    ALTER COLUMN consumed_at DROP DEFAULT;

CREATE INDEX IF NOT EXISTS idx_message_consumer_inbox_event
    ON message_consumer_inbox (event_id, consumed_at DESC);

-- Message publication and consumption audit trail.
CREATE TABLE IF NOT EXISTS message_delivery_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL,
    consumer_name TEXT,
    delivery_status TEXT NOT NULL,
    retry_count INT NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    error_message TEXT,
    delivered_at TIMESTAMPTZ,
    consumed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_message_delivery_audit_event
    ON message_delivery_audit (event_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_message_delivery_audit_consumer
    ON message_delivery_audit (consumer_name, created_at DESC)
    WHERE consumer_name IS NOT NULL;

-- Immutable execution lifecycle and Agent Card audit events.
CREATE SEQUENCE IF NOT EXISTS task_execution_event_sequence;

CREATE TABLE IF NOT EXISTS task_execution_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sequence_no BIGINT NOT NULL DEFAULT nextval('task_execution_event_sequence'),
    task_id TEXT NOT NULL,
    execution_id UUID NOT NULL,
    attempt INT NOT NULL,
    event_id UUID,
    correlation_id UUID,
    runtime_id TEXT,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (task_id, execution_id, attempt)
        REFERENCES task_executions (task_id, execution_id, attempt),
    FOREIGN KEY (runtime_id) REFERENCES agent_runtimes(runtime_id)
);

ALTER TABLE task_execution_events
    ADD COLUMN IF NOT EXISTS sequence_no BIGINT;
UPDATE task_execution_events
SET sequence_no = nextval('task_execution_event_sequence')
WHERE sequence_no IS NULL;
ALTER TABLE task_execution_events
    ALTER COLUMN sequence_no SET DEFAULT nextval('task_execution_event_sequence');
ALTER TABLE task_execution_events
    ALTER COLUMN sequence_no SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_task_execution_events_execution
    ON task_execution_events (execution_id, attempt, sequence_no);

CREATE INDEX IF NOT EXISTS idx_task_execution_events_task
    ON task_execution_events (task_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_task_execution_events_event_id
    ON task_execution_events (event_id)
    WHERE event_id IS NOT NULL;

-- Existing permission audit schema, extended with execution context.
CREATE TABLE IF NOT EXISTS permission_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id VARCHAR(255) UNIQUE NOT NULL,
    agent_name VARCHAR(255) NOT NULL,
    tool_name VARCHAR(100) NOT NULL,
    command TEXT NOT NULL,
    decision VARCHAR(20),
    reason TEXT,
    decided_by VARCHAR(255),
    thread_id TEXT,
    task_id TEXT,
    execution_id UUID,
    attempt INT,
    event_id UUID,
    correlation_id UUID,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    decided_at TIMESTAMPTZ
);

ALTER TABLE permission_audit_log ADD COLUMN IF NOT EXISTS thread_id TEXT;
ALTER TABLE permission_audit_log ADD COLUMN IF NOT EXISTS task_id TEXT;
ALTER TABLE permission_audit_log ADD COLUMN IF NOT EXISTS execution_id UUID;
ALTER TABLE permission_audit_log ADD COLUMN IF NOT EXISTS attempt INT;
ALTER TABLE permission_audit_log ADD COLUMN IF NOT EXISTS event_id UUID;
ALTER TABLE permission_audit_log ADD COLUMN IF NOT EXISTS correlation_id UUID;

ALTER TABLE permission_audit_log DROP CONSTRAINT IF EXISTS permission_audit_log_execution_context_check;
ALTER TABLE permission_audit_log ADD CONSTRAINT permission_audit_log_execution_context_check
    CHECK (
        (task_id IS NULL AND execution_id IS NULL AND attempt IS NULL)
        OR (task_id IS NOT NULL AND execution_id IS NOT NULL AND attempt IS NOT NULL)
    );

ALTER TABLE permission_audit_log DROP CONSTRAINT IF EXISTS permission_audit_log_execution_fkey;
ALTER TABLE permission_audit_log ADD CONSTRAINT permission_audit_log_execution_fkey
    FOREIGN KEY (task_id, execution_id, attempt)
    REFERENCES task_executions (task_id, execution_id, attempt);

CREATE INDEX IF NOT EXISTS idx_permission_audit_agent
    ON permission_audit_log (agent_name, created_at);

CREATE INDEX IF NOT EXISTS idx_permission_audit_decision
    ON permission_audit_log (decision, created_at);

CREATE INDEX IF NOT EXISTS idx_permission_audit_execution
    ON permission_audit_log (execution_id, attempt, created_at)
    WHERE execution_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_permission_audit_event
    ON permission_audit_log (event_id)
    WHERE event_id IS NOT NULL;

COMMIT;
