-- Drop unused site-only index (PK already covers product_id+site queries)
DROP INDEX IF EXISTS idx_latest_prices_site;

-- Partial index for unread alerts (stays small as alerts are marked read)
CREATE INDEX idx_alerts_unread
    ON alerts(created_at DESC)
    WHERE is_read = FALSE;

-- Supports alert_type + date range queries in get_summary_stats
CREATE INDEX idx_alerts_type_created
    ON alerts(alert_type, created_at DESC);

-- Domain constraints
ALTER TABLE alerts
    ADD CONSTRAINT chk_alert_type
    CHECK (alert_type IN ('NEW_LOW', 'NEW_HIGH', 'PRICE_DROP', 'PRICE_SPIKE'));

ALTER TABLE latest_prices
    ADD CONSTRAINT chk_site
    CHECK (site IN ('danawa', 'compuzone', 'pc_estimate'));

ALTER TABLE alerts
    ADD CONSTRAINT chk_alert_site
    CHECK (site IN ('danawa', 'compuzone', 'pc_estimate'));

-- Append-only price history
CREATE TABLE price_history (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id  UUID NOT NULL REFERENCES products(product_id),
    site        text NOT NULL,
    price       INTEGER NOT NULL,
    url         TEXT,
    crawled_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_price_history_lookup
    ON price_history(product_id, site, crawled_at DESC);
