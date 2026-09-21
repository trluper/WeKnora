-- Migration: 000109_datahub_test_tables
-- Datahub's test-data tables: one row per board (Test detail) and one row per
-- Event (Test summary). Both belong to the Datahub module alone.
--
-- The summary is per Event, not per person: it is the Event's overall progress,
-- aggregated across everyone who recorded board results for it. It is
-- maintained incrementally, in the same transaction as the details it counts,
-- with a periodic full recompute as the self-healing backstop.

CREATE TABLE IF NOT EXISTS datahub_test_details (
    id            bigserial    PRIMARY KEY,
    tenant_id     bigint       NOT NULL,
    event_id      varchar(64)  NOT NULL,
    board_id      varchar(64)  NOT NULL,
    -- 1 = passed, 0 = failed. Anything else is rejected before it gets here.
    test_result   smallint     NOT NULL,
    failed_reason varchar(255) NOT NULL DEFAULT '',
    user_id       varchar(36)  NOT NULL,
    created_at    timestamptz  NOT NULL DEFAULT now(),
    updated_at    timestamptz  NOT NULL DEFAULT now(),
    -- Re-reporting a board is a correction, so this is an upsert key, not a
    -- duplicate to be stored twice.
    CONSTRAINT uk_datahub_test_details_event_board
        UNIQUE (tenant_id, event_id, board_id)
);

-- Detail listings page one Event backwards by time.
CREATE INDEX IF NOT EXISTS idx_datahub_test_details_tenant_event_created
    ON datahub_test_details (tenant_id, event_id, created_at);
CREATE INDEX IF NOT EXISTS idx_datahub_test_details_tenant_user
    ON datahub_test_details (tenant_id, user_id);

CREATE TABLE IF NOT EXISTS datahub_test_summaries (
    id           bigserial   PRIMARY KEY,
    tenant_id    bigint      NOT NULL,
    event_id     varchar(64) NOT NULL,
    total_count  bigint      NOT NULL DEFAULT 0,
    passed_count bigint      NOT NULL DEFAULT 0,
    failed_count bigint      NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uk_datahub_test_summaries_event UNIQUE (tenant_id, event_id)
);
