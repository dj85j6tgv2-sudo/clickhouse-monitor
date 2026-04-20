-- ============================================================
-- Users (SQL-managed, not XML)
-- ============================================================
CREATE USER IF NOT EXISTS monitor   IDENTIFIED BY 'monitor123';
CREATE USER IF NOT EXISTS alice     IDENTIFIED BY 'alice123';
CREATE USER IF NOT EXISTS bob       IDENTIFIED BY 'bob123';
CREATE USER IF NOT EXISTS svc_etl   IDENTIFIED BY 'etl123';

GRANT SELECT ON system.*             TO monitor;
GRANT SELECT ON information_schema.* TO monitor;
GRANT REMOTE ON *.*                  TO monitor;
GRANT CLUSTER ON *.*                 TO monitor;

-- ============================================================
-- Sample databases and tables
-- ============================================================
CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS analytics.page_views
(
    event_date   Date,
    event_time   DateTime,
    user_id      UInt64,
    session_id   String,
    page         LowCardinality(String),
    referrer     LowCardinality(String),
    country      LowCardinality(String),
    duration_ms  UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id)
TTL event_date + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS analytics.orders
(
    order_id     UInt64,
    created_at   DateTime,
    user_id      UInt64,
    total_amount Decimal(10, 2),
    status       LowCardinality(String),
    items        UInt16
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, user_id);

CREATE TABLE IF NOT EXISTS events.user_events
(
    event_time   DateTime,
    user_id      UInt64,
    event_type   LowCardinality(String),
    properties   String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

-- ============================================================
-- Seed: page_views (~500k rows)
-- ============================================================
INSERT INTO analytics.page_views
SELECT
    today() - (rand() % 60)       AS event_date,
    now() - (rand() % 5184000)    AS event_time,
    rand() % 10000                 AS user_id,
    toString(rand())               AS session_id,
    arrayElement(['/home', '/products', '/cart', '/checkout', '/about', '/search'], rand() % 6 + 1) AS page,
    arrayElement(['google.com', 'twitter.com', 'direct', 'facebook.com'], rand() % 4 + 1) AS referrer,
    arrayElement(['US', 'BR', 'FR', 'DE', 'GB', 'CA', 'JP'], rand() % 7 + 1) AS country,
    rand() % 30000                 AS duration_ms
FROM numbers(500000);

-- ============================================================
-- Seed: orders (~50k rows)
-- ============================================================
INSERT INTO analytics.orders
SELECT
    rand() % 1000000               AS order_id,
    now() - (rand() % 5184000)    AS created_at,
    rand() % 10000                 AS user_id,
    (rand() % 100000) / 100.0     AS total_amount,
    arrayElement(['pending', 'processing', 'shipped', 'delivered', 'cancelled'], rand() % 5 + 1) AS status,
    rand() % 10 + 1               AS items
FROM numbers(50000);

-- ============================================================
-- Seed: user_events (~200k rows)
-- ============================================================
INSERT INTO events.user_events
SELECT
    now() - (rand() % 2592000)    AS event_time,
    rand() % 10000                 AS user_id,
    arrayElement(['click', 'view', 'purchase', 'signup', 'logout', 'search'], rand() % 6 + 1) AS event_type,
    '{"source":"web"}'             AS properties
FROM numbers(200000);

-- ============================================================
-- Grant access to sample tables
-- ============================================================
GRANT SELECT ON analytics.* TO monitor, alice, bob, svc_etl;
GRANT SELECT ON events.*    TO monitor, alice, bob, svc_etl;

-- ============================================================
-- Simulate query_log activity from alice, bob, svc_etl
-- (run a few queries as each user so they appear in logs)
-- ============================================================
SELECT count() FROM analytics.page_views;
SELECT count() FROM analytics.orders;
