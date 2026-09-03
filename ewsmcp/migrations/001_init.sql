-- ews schema v1: the mirror tables (ported from the 4.5 SQLite schema),
-- the alias map, and the archive columns Phase 2 will fill.
CREATE TABLE ews.meta (
    key   text PRIMARY KEY,
    value text
);

CREATE TABLE ews.messages (
    ews_id              text PRIMARY KEY,
    changekey           text,
    folder              text NOT NULL,
    conversation_id     text,
    sender_name         text,
    sender_email        text,
    to_json             text,
    subject             text,
    date_ts             bigint,
    date_iso            text,
    is_read             smallint NOT NULL DEFAULT 1,
    has_attachments     smallint NOT NULL DEFAULT 0,
    importance          text,
    categories_json     text,
    body_clean          text,
    internet_message_id text,
    norm_text           text NOT NULL DEFAULT '',
    archive_state       text NOT NULL DEFAULT 'live'
                        CHECK (archive_state IN ('live', 'captured', 'verified', 'deleted')),
    archived_at         timestamptz,
    verified_at         timestamptz,
    deleted_at          timestamptz,
    mime_sha256         text,
    mime_path           text,
    embedded_at         timestamptz,
    search_tsv          tsvector GENERATED ALWAYS AS (to_tsvector('simple', norm_text)) STORED
);
CREATE INDEX ix_msg_folder_date  ON ews.messages (folder, date_ts DESC);
CREATE INDEX ix_msg_conversation ON ews.messages (conversation_id);
CREATE INDEX ix_msg_sender       ON ews.messages (lower(sender_email));
CREATE INDEX ix_msg_imid         ON ews.messages (internet_message_id);
CREATE INDEX ix_msg_state        ON ews.messages (archive_state);
CREATE INDEX ix_msg_tsv          ON ews.messages USING GIN (search_tsv);

CREATE TABLE ews.events (
    ews_id       text PRIMARY KEY,
    changekey    text,
    subject      text,
    start_ts     bigint,
    start_iso    text,
    end_ts       bigint,
    end_iso      text,
    location     text,
    organizer    text,
    is_recurring smallint NOT NULL DEFAULT 0,
    my_response  text
);
CREATE INDEX ix_events_start ON ews.events (start_ts);

CREATE TABLE ews.tasks (
    ews_id      text PRIMARY KEY,
    changekey   text,
    subject     text,
    due_ts      bigint,
    due_iso     text,
    is_complete smallint NOT NULL DEFAULT 0,
    status      text
);

CREATE TABLE ews.folders (
    ews_id   text PRIMARY KEY,
    name     text,
    path     text,
    wk       text,
    total    integer,
    unread   integer,
    children integer
);

CREATE TABLE ews.sync_state (
    key   text PRIMARY KEY,
    token text,
    as_of bigint
);

CREATE TABLE ews.sender_sigs (
    sender_email text NOT NULL,
    sig_hash     text NOT NULL,
    hits         integer NOT NULL DEFAULT 1,
    PRIMARY KEY (sender_email, sig_hash)
);

CREATE TABLE ews.aliases (
    alias               text PRIMARY KEY,
    kind                text NOT NULL,
    ews_id              text NOT NULL UNIQUE,
    changekey           text,
    internet_message_id text,
    first_seen          double precision,
    last_seen           double precision
);
CREATE INDEX ix_aliases_imid ON ews.aliases (internet_message_id);

CREATE TABLE ews.alias_counters (
    kind text PRIMARY KEY,
    n    integer NOT NULL
);
