-- PHASE 3: Prepare PostgreSQL
-- Step A: run this line alone
CREATE DATABASE ecommerce_events;

-- Step B: connect to ecommerce_events, then run the rest

-- Table for the processed events
CREATE TABLE IF NOT EXISTS events (
    event_id    VARCHAR(50) PRIMARY KEY,   -- unique: blocks duplicates
    user_id     VARCHAR(50) NOT NULL,
    event_type  VARCHAR(20) NOT NULL,      -- 'view' or 'purchase'
    product_id  VARCHAR(50) NOT NULL,
    event_time  TIMESTAMP NOT NULL,        -- when the customer acted
    amount      DECIMAL(10,2),             -- empty for views
    inserted_at TIMESTAMP DEFAULT NOW()    -- when it reached the database
);

-- Table for performance evidence (one row per Spark batch)
CREATE TABLE IF NOT EXISTS batch_metrics (
    batch_id      INT,
    rows_received INT,
    rows_valid    INT,
    rows_inserted INT,
    seconds       DECIMAL(10,3),
    finished_at   TIMESTAMP DEFAULT NOW()
);
