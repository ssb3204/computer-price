USE DATABASE COMPUTER_PRICE;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS PRICE_CHANGES (
    change_id       VARCHAR(36) NOT NULL,
    product_id      VARCHAR(36) NOT NULL,
    product_name    VARCHAR(500) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    site            VARCHAR(50) NOT NULL,
    old_price       NUMBER(10,0),
    new_price       NUMBER(10,0) NOT NULL,
    change_amount   NUMBER(10,0),
    change_pct      NUMBER(8,4),
    url             VARCHAR(2000),
    crawled_at      TIMESTAMP_TZ NOT NULL,
    loaded_at       TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DAILY_SNAPSHOTS (
    snapshot_date   DATE NOT NULL,
    product_id      VARCHAR(36) NOT NULL,
    product_name    VARCHAR(500) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    site            VARCHAR(50) NOT NULL,
    price           NUMBER(10,0) NOT NULL,
    url             VARCHAR(2000),
    loaded_at       TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, product_id, site)
);
