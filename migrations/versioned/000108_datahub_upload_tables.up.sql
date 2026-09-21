-- Migration: 000108_datahub_upload_tables
-- Datahub's upload tables. Datahub is a self-contained module: these tables
-- belong to it alone, and nothing outside internal/datahub reads or writes
-- them. See docs/adr/0001-upload-record-merge.md.
--
-- One Upload is one row in datahub_uploads for its whole life: the multipart
-- session it opened, the object-storage facts it produced, the product
-- metadata the client submitted, and the columns reserved for the semantic
-- work a future summarizer will do.
--
-- Column ownership is load-bearing. Everything from the tenant keys down to
-- expire_at is written by the upload flow. The reserved block at the bottom has
-- no writer today beyond the empty value an insert leaves behind, and must not
-- be read as a live feature.

CREATE TABLE IF NOT EXISTS datahub_uploads (
    id                bigserial     PRIMARY KEY,
    tenant_id         bigint        NOT NULL,
    upload_id         varchar(64)   NOT NULL,
    event_id          varchar(64)   NOT NULL,
    scenario          varchar(32)   NOT NULL DEFAULT 'experiment',
    user_id           varchar(36)   NOT NULL,
    filename          varchar(255)  NOT NULL,
    file_size         bigint        NOT NULL,
    file_hash         varchar(128)  NOT NULL DEFAULT '',
    content_type      varchar(128)  NOT NULL DEFAULT '',
    bucket            varchar(64)   NOT NULL DEFAULT '',
    object_key        varchar(512)  NOT NULL DEFAULT '',
    storage_path      varchar(512)  NOT NULL DEFAULT '',
    -- S3/MinIO multipart upload id. Named for the protocol, not the vendor:
    -- this module runs against any S3-compatible store.
    object_upload_id  varchar(128)  NOT NULL DEFAULT '',
    status            varchar(32)   NOT NULL,
    total_parts       integer       NOT NULL DEFAULT 1,
    completed_parts   integer       NOT NULL DEFAULT 0,
    part_size         bigint        NOT NULL DEFAULT 5242880,
    metadata          jsonb,

    -- Product metadata: submitted by the client when finishing an upload.
    description       varchar(2000) NOT NULL DEFAULT '',
    category          varchar(64)   NOT NULL DEFAULT '',
    important_key     varchar(2000) NOT NULL DEFAULT '',
    version           varchar(64)   NOT NULL DEFAULT '',

    -- Object-storage facts, written when the multipart session completes.
    etag              varchar(255)  NOT NULL DEFAULT '',
    object_version_id varchar(128)  NOT NULL DEFAULT '',
    last_modified     timestamptz,

    expire_at         timestamptz   NOT NULL,
    completed_at      timestamptz,
    created_at        timestamptz   NOT NULL DEFAULT now(),
    updated_at        timestamptz   NOT NULL DEFAULT now(),

    -- Reserved for a future summarizer. Only error_msg has a writer today: the
    -- upload flow records why a merge failed. The upload flow must never write
    -- the other columns, so a re-run cannot clobber summarizer output.
    summary_markdown  text,
    headline          varchar(512),
    keywords          jsonb,
    analysis_state    varchar(32),
    queued_at         timestamptz,
    started_at        timestamptz,
    finished_at       timestamptz,
    error_msg         text,

    CONSTRAINT uk_datahub_uploads_tenant_upload UNIQUE (tenant_id, upload_id)
);

-- Listings are always tenant-scoped; every index leads with tenant_id.
CREATE INDEX IF NOT EXISTS idx_datahub_uploads_tenant_event
    ON datahub_uploads (tenant_id, event_id);
CREATE INDEX IF NOT EXISTS idx_datahub_uploads_tenant_user
    ON datahub_uploads (tenant_id, user_id);
CREATE INDEX IF NOT EXISTS idx_datahub_uploads_tenant_status
    ON datahub_uploads (tenant_id, status);
-- Reconciler sweeps unfinished uploads by expiry across tenants.
CREATE INDEX IF NOT EXISTS idx_datahub_uploads_expire_at
    ON datahub_uploads (expire_at);

-- Part-registration state for one Upload: a bitmap of which parts arrived plus
-- the JSON-encoded part metadata, both rewritten under the row lock that also
-- serialises concurrent registrations.
CREATE TABLE IF NOT EXISTS datahub_upload_parts (
    tenant_id       bigint       NOT NULL,
    upload_id       varchar(64)  NOT NULL,
    part_bitmap     bytea        NOT NULL,
    part_meta       bytea,
    completed_parts integer      NOT NULL DEFAULT 0,
    version         integer      NOT NULL DEFAULT 0,
    is_merged       smallint     NOT NULL DEFAULT 0,
    created_at      timestamptz  NOT NULL DEFAULT now(),
    updated_at      timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, upload_id)
);
