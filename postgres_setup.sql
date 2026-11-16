-- PostgreSQL setup for the e-commerce streaming lab (database: ecommerce_events)
-- Docker runs this file automatically the first time the PostgreSQL container starts.
-- (Without Docker: create the database ecommerce_events first, connect to it, then run this file.)

-- 1. Clean events: one row per valid event (Spark writes here)
CREATE TABLE IF NOT EXISTS events (
    event_id        VARCHAR(50)   PRIMARY KEY,                -- unique: blocks duplicates
    user_id         VARCHAR(20)   NOT NULL,
    event_type      VARCHAR(20)   NOT NULL
                    CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    product_id      VARCHAR(20)   NOT NULL,
    category        VARCHAR(30)   NOT NULL,
    price           NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    quantity        INT           NOT NULL CHECK (quantity > 0),
    event_time      TIMESTAMP     NOT NULL,                   -- when the customer acted
    -- derived columns (calculated by Spark)
    total_amount    NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),  -- price x quantity, only for purchases
    is_purchase     BOOLEAN       NOT NULL,
    event_date      DATE          NOT NULL,
    event_hour      INT           NOT NULL CHECK (event_hour BETWEEN 0 AND 23),
    ingestion_time  TIMESTAMP     NOT NULL,                   -- when Spark processed it
    latency_seconds NUMERIC(10,2),                            -- ingestion_time - event_time
    inserted_at     TIMESTAMP     DEFAULT NOW()               -- when it reached the database
);

-- Indexes: make common questions fast ("events in the last hour", "what did this user do?")
CREATE INDEX IF NOT EXISTS idx_events_event_time ON events (event_time);
CREATE INDEX IF NOT EXISTS idx_events_user_id    ON events (user_id);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);

-- 2. Rejected events: bad rows are kept with the reason (not silently dropped)
--    All columns are text, because bad data may not fit the right types
CREATE TABLE IF NOT EXISTS rejected_events (
    id          SERIAL PRIMARY KEY,
    event_id    TEXT,
    user_id     TEXT,
    event_type  TEXT,
    product_id  TEXT,
    category    TEXT,
    price       TEXT,
    quantity    TEXT,
    event_time  TEXT,
    reason      TEXT NOT NULL,
    batch_id    BIGINT,
    rejected_at TIMESTAMP DEFAULT NOW()
);

-- 3. Window results: events and revenue per 1-minute window (watermark + window aggregation)
--    The primary key lets us UPSERT: if a late event changes a window, we update that row
CREATE TABLE IF NOT EXISTS window_metrics (
    window_start  TIMESTAMP     NOT NULL,
    window_end    TIMESTAMP     NOT NULL,
    event_type    VARCHAR(20)   NOT NULL,
    event_count   BIGINT        NOT NULL CHECK (event_count >= 0),
    total_revenue NUMERIC(14,2) NOT NULL CHECK (total_revenue >= 0),
    updated_at    TIMESTAMP     DEFAULT NOW(),
    PRIMARY KEY (window_start, event_type)
);

-- 4. Batch metrics: one row per Spark batch (my own counts)
CREATE TABLE IF NOT EXISTS batch_metrics (
    batch_id      BIGINT,
    rows_received INT,
    rows_valid    INT,
    rows_rejected INT,
    rows_inserted INT,
    seconds       NUMERIC(10,3),
    finished_at   TIMESTAMP DEFAULT NOW()
);

-- 5. Stream progress: Spark's own measurements from query.lastProgress
CREATE TABLE IF NOT EXISTS stream_progress (
    query_name                VARCHAR(50),
    batch_id                  BIGINT,
    num_input_rows            BIGINT,
    input_rows_per_second     NUMERIC(14,2),
    processed_rows_per_second NUMERIC(14,2),
    trigger_duration_ms       BIGINT,          -- how long the whole batch took
    add_batch_ms              BIGINT,          -- time spent in foreachBatch (writing)
    recorded_at               TIMESTAMP DEFAULT NOW()
);
