-- ============================================================================
-- Voyage BI Copilot — warehouse schema
-- Idempotent: safe to run repeatedly (IF NOT EXISTS / DO blocks).
-- ============================================================================

-- Extensions ------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS vector;

-- Schema ----------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Read-only role for MCP server -----------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bi_copilot_ro') THEN
    CREATE ROLE bi_copilot_ro WITH LOGIN PASSWORD 'voyage_ro';
  END IF;
END
$$;

-- ============================================================================
-- Core tables (generation order respects FK deps)
-- ============================================================================

CREATE TABLE IF NOT EXISTS warehouse.markets (
    market_id  SERIAL      PRIMARY KEY,
    name       TEXT        NOT NULL,
    state      TEXT        NOT NULL,
    timezone   TEXT        NOT NULL,   -- IANA tz, e.g. 'America/Los_Angeles'
    region     TEXT        NOT NULL
);
COMMENT ON TABLE  warehouse.markets IS 'Vacation rental markets — geographic groupings of properties.';
COMMENT ON COLUMN warehouse.markets.timezone IS 'IANA timezone string used for local-time calculations.';

-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.owners (
    owner_id     SERIAL      PRIMARY KEY,
    name         TEXT        NOT NULL,
    onboarded_at TIMESTAMPTZ NOT NULL
);
COMMENT ON TABLE warehouse.owners IS 'Property owners enrolled on the platform.';

-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.properties (
    property_id       SERIAL        PRIMARY KEY,
    market_id         INTEGER       NOT NULL REFERENCES warehouse.markets(market_id),
    owner_id          INTEGER       NOT NULL REFERENCES warehouse.owners(owner_id),
    bedrooms          INTEGER       NOT NULL,
    max_occupancy     INTEGER       NOT NULL,
    nightly_base_rate NUMERIC(10,2) NOT NULL,
    listed_at         TIMESTAMPTZ   NOT NULL,
    delisted_at       TIMESTAMPTZ             -- NULL = still active
);
COMMENT ON TABLE  warehouse.properties  IS 'Individual rental properties. delisted_at IS NULL for active listings.';
COMMENT ON COLUMN warehouse.properties.delisted_at IS 'NULL means the property is currently active.';

-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.reservations (
    reservation_id SERIAL        PRIMARY KEY,
    property_id    INTEGER       NOT NULL REFERENCES warehouse.properties(property_id),
    guest_id       INTEGER       NOT NULL,   -- opaque anonymised guest identifier
    channel        TEXT          NOT NULL CHECK (channel IN ('airbnb','vrbo','direct','marriott')),
    check_in       DATE          NOT NULL,
    check_out      DATE          NOT NULL,
    nights         INTEGER       NOT NULL,
    gross_revenue  NUMERIC(10,2) NOT NULL,   -- before channel fees
    net_revenue    NUMERIC(10,2) NOT NULL,   -- after channel fees
    booking_date   TIMESTAMPTZ   NOT NULL,
    status         TEXT          NOT NULL CHECK (status IN ('confirmed','cancelled'))
);
COMMENT ON TABLE  warehouse.reservations IS
    'All reservations including cancellations. '
    'Filter status = ''confirmed'' for revenue metrics. '
    'net_revenue < gross_revenue due to channel fees.';
COMMENT ON COLUMN warehouse.reservations.gross_revenue IS 'Revenue before channel commission fees.';
COMMENT ON COLUMN warehouse.reservations.net_revenue   IS 'Revenue after channel commission fees.';

-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.pricing_snapshots (
    property_id   INTEGER       NOT NULL REFERENCES warehouse.properties(property_id),
    snapshot_date DATE          NOT NULL,
    nightly_rate  NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (property_id, snapshot_date)
);
COMMENT ON TABLE warehouse.pricing_snapshots IS 'One row per property per day showing the listed nightly rate.';

-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.reviews (
    review_id      SERIAL      PRIMARY KEY,
    reservation_id INTEGER     NOT NULL REFERENCES warehouse.reservations(reservation_id),
    rating         INTEGER     NOT NULL CHECK (rating BETWEEN 1 AND 5),
    sentiment      TEXT                 CHECK (sentiment IN ('positive','neutral','negative')),
    submitted_at   TIMESTAMPTZ NOT NULL
);
COMMENT ON TABLE  warehouse.reviews IS 'Guest reviews linked to confirmed reservations.';
COMMENT ON COLUMN warehouse.reviews.sentiment IS
    'Precomputed sentiment label. NULL for reviews not yet scored.';

-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.sensor_events (
    event_id    SERIAL      PRIMARY KEY,
    property_id INTEGER     NOT NULL REFERENCES warehouse.properties(property_id),
    event_type  TEXT        NOT NULL
        CHECK (event_type IN ('noise_spike','occupancy_mismatch','door_access','lock_failure')),
    severity    INTEGER     NOT NULL CHECK (severity BETWEEN 1 AND 5),
    occurred_at TIMESTAMPTZ NOT NULL
);
COMMENT ON TABLE warehouse.sensor_events IS
    'IoT sensor events. Useful for cross-domain queries with reviews, '
    'e.g. noise complaints at properties with recent negative reviews.';

-- ============================================================================
-- Retrieval index (pgvector embeddings — lives in public schema)
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.retrieval_embeddings (
    id        SERIAL       PRIMARY KEY,
    kind      TEXT         NOT NULL,   -- 'table_description' | 'few_shot'
    label     TEXT         NOT NULL,   -- table name or short question string
    content   TEXT         NOT NULL,   -- full text that was embedded
    embedding vector(1536),            -- OpenAI text-embedding-3-small
    metadata  JSONB        NOT NULL DEFAULT '{}'
);
COMMENT ON TABLE public.retrieval_embeddings IS
    'Vector embeddings for schema retrieval and few-shot example lookup.';

-- ============================================================================
-- Indexes
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_res_property   ON warehouse.reservations(property_id);
CREATE INDEX IF NOT EXISTS idx_res_check_in   ON warehouse.reservations(check_in);
CREATE INDEX IF NOT EXISTS idx_res_check_out  ON warehouse.reservations(check_out);
CREATE INDEX IF NOT EXISTS idx_res_status     ON warehouse.reservations(status);
CREATE INDEX IF NOT EXISTS idx_res_channel    ON warehouse.reservations(channel);
CREATE INDEX IF NOT EXISTS idx_price_prop_dt  ON warehouse.pricing_snapshots(property_id, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_rev_res        ON warehouse.reviews(reservation_id);
CREATE INDEX IF NOT EXISTS idx_sensor_prop_ts ON warehouse.sensor_events(property_id, occurred_at);

-- ============================================================================
-- Grants
-- ============================================================================
GRANT USAGE ON SCHEMA warehouse TO bi_copilot_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO bi_copilot_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse GRANT SELECT ON TABLES TO bi_copilot_ro;
