-- PostgreSQL initial schema

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE products (
    product_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(500) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    brand           VARCHAR(100),
    model_number    VARCHAR(200),
    normalized_name VARCHAR(500) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(normalized_name)
);

CREATE TABLE latest_prices (
    product_id      UUID NOT NULL REFERENCES products(product_id),
    site            VARCHAR(50) NOT NULL,
    price           INTEGER NOT NULL,
    url             TEXT,
    crawled_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (product_id, site)
);

CREATE TABLE alerts (
    alert_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id      UUID NOT NULL REFERENCES products(product_id),
    alert_type      VARCHAR(30) NOT NULL,
    site            VARCHAR(50) NOT NULL,
    old_price       INTEGER,
    new_price       INTEGER NOT NULL,
    change_pct      DECIMAL(8,4),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_read         BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_alerts_created ON alerts(created_at DESC);
CREATE INDEX idx_alerts_product ON alerts(product_id);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_latest_prices_site ON latest_prices(site);
