-- =====================================================
--  computer_price — MySQL 8.0 통합 스키마 (Phase 2)
--  근거: docs/MYSQL_MODELING_GUIDE.md (확정본)
--
--  규칙:
--    - 단일 DB(computer_price) + 접두사(raw_/stg_/ans_) 3계층
--    - CREATE DATABASE / USER 구문 없음 (이미 완료, price_app로 적용)
--    - 모든 식별자 백틱 인용, ENGINE=InnoDB, utf8mb4
--    - 대리키 BIGINT AUTO_INCREMENT PK, 자연키는 UNIQUE, append-only
--    - 생성 순서: 참조 대상 먼저 → FK 테이블 → 뷰(마지막)
--
--  제외: BUILD*/USER 계열(미구현), PIPELINE_RUNS/STEP_RUNS(죽은 테이블),
--        STOCK_STATUS 컬럼(죽은 컬럼)
-- =====================================================

-- ─────────────────────────────────────────────────────
--  LAYER 1: RAW — 크롤링 원본 (가공 없음)
-- ─────────────────────────────────────────────────────

-- 크롤러 산출 RawCrawledPrice 원본 적재. load_raw.py가 INSERT.
CREATE TABLE IF NOT EXISTS `raw_crawled_prices` (
    `id`           BIGINT        NOT NULL AUTO_INCREMENT,
    `site`         VARCHAR(50)   NOT NULL,                     -- 영문(danawa/compuzone/kjwwang)
    `category`     VARCHAR(100)  NOT NULL,
    `product_name` VARCHAR(500)  NOT NULL,                     -- §7-1 확정: 500
    `price_text`   VARCHAR(100)  NOT NULL,                     -- 원본 텍스트, 불변 계약
    `brand`        VARCHAR(200)  NULL,
    `url`          VARCHAR(4000) NOT NULL,
    `crawled_at`   DATETIME      NOT NULL,                     -- UTC
    `loaded_at`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_raw_crawled` (`site`, `category`, `product_name`, `crawled_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- transform 파싱/이상치 실패 감사(append-only). transform.py가 INSERT.
CREATE TABLE IF NOT EXISTS `raw_transform_failures` (
    `id`                BIGINT        NOT NULL AUTO_INCREMENT,
    `crawled_prices_id` BIGINT        NULL,                    -- raw_crawled_prices.id 참조값(FK 아님)
    `site`              VARCHAR(50)   NULL,
    `category`          VARCHAR(100)  NULL,
    `product_name`      VARCHAR(500)  NULL,                    -- §7-1 확정: 500 통일
    `price_text`        VARCHAR(100)  NULL,
    `crawled_at`        DATETIME      NULL,
    `reject_reason`     VARCHAR(200)  NOT NULL,
    `failed_at`         DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- ─────────────────────────────────────────────────────
--  LAYER 2: STAGING — 정제/비정규화
-- ─────────────────────────────────────────────────────

-- 정제된 상품 마스터. transform.py가 UPSERT. (다른 stg/ans 테이블이 참조 → 먼저 생성)
CREATE TABLE IF NOT EXISTS `stg_products` (
    `product_id`   BIGINT        NOT NULL AUTO_INCREMENT,
    `site`         VARCHAR(50)   NOT NULL,                     -- 한글 표시명(다나와/컴퓨존/견적왕)
    `category`     VARCHAR(100)  NOT NULL,
    `product_name` VARCHAR(500)  NOT NULL,
    `url`          VARCHAR(4000) NULL,
    `created_at`   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`product_id`),
    UNIQUE KEY `uq_products` (`site`, `product_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 상품별 가격 이력(append-only). transform.py가 UPSERT.
CREATE TABLE IF NOT EXISTS `stg_price_history` (
    `id`         BIGINT   NOT NULL AUTO_INCREMENT,
    `product_id` BIGINT   NOT NULL,
    `raw_id`     BIGINT   NULL,                                -- raw_crawled_prices.id 추적(FK 아님)
    `price`      BIGINT   NOT NULL,                            -- KRW 정수
    `crawled_at` DATETIME NOT NULL,                            -- UTC
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_price_history` (`product_id`, `crawled_at`),  -- §7-2 확정: 신규 추가
    CONSTRAINT `fk_price_history_product`
        FOREIGN KEY (`product_id`) REFERENCES `stg_products` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 가격 변동 알림(append-only). detect.py가 INSERT.
CREATE TABLE IF NOT EXISTS `stg_price_alerts` (
    `alert_id`       BIGINT       NOT NULL AUTO_INCREMENT,
    `product_id`     BIGINT       NOT NULL,
    `daily_price_id` BIGINT       NULL,                        -- 중복 알림 방지 키(detect NOT EXISTS)
    `alert_type`     VARCHAR(30)  NOT NULL,                    -- NEW_LOW/NEW_HIGH/PRICE_DROP/PRICE_SPIKE
    `old_price`      BIGINT       NULL,
    `new_price`      BIGINT       NOT NULL,
    `change_pct`     DECIMAL(8,4) NULL,
    `created_at`     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`alert_id`),
    CONSTRAINT `fk_price_alerts_product`
        FOREIGN KEY (`product_id`) REFERENCES `stg_products` (`product_id`),
    CONSTRAINT `fk_price_alerts_history`
        FOREIGN KEY (`daily_price_id`) REFERENCES `stg_price_history` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 크롤링 대상 목록. 대시보드가 관리, 크롤러가 읽기.
CREATE TABLE IF NOT EXISTS `stg_watchlist` (
    `id`           BIGINT       NOT NULL AUTO_INCREMENT,
    `query`        VARCHAR(500) NOT NULL,                      -- 검색어(백틱 인용, §7-4)
    `pcode`        VARCHAR(50)  NOT NULL,                      -- 사이트별 상품ID 공용(pcode/product_no/pd_no)
    `product_name` VARCHAR(500) NULL,
    `category`     VARCHAR(100) NOT NULL,
    `brand`        VARCHAR(200) NULL,
    `site`         VARCHAR(20)  NOT NULL DEFAULT '다나와',      -- 한글값
    `is_active`    TINYINT(1)   NOT NULL DEFAULT 1,
    `added_at`     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_watchlist_pcode` (`pcode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- ─────────────────────────────────────────────────────
--  LAYER 3: ANALYTICS — 시각화/분석용 집계
-- ─────────────────────────────────────────────────────

-- 일별 집계. analytics.py가 UPSERT (GROUP BY product_id, DATE(crawled_at)).
CREATE TABLE IF NOT EXISTS `ans_daily_price_stats` (
    `id`           BIGINT       NOT NULL AUTO_INCREMENT,
    `product_id`   BIGINT       NOT NULL,
    `price_date`   DATE         NOT NULL,
    `min_price`    BIGINT       NOT NULL,
    `max_price`    BIGINT       NOT NULL,
    `avg_price`    DECIMAL(12,2) NOT NULL,
    `record_count` BIGINT       NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_daily_price_stats` (`product_id`, `price_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 주별 집계. week_start = 월요일 시작(§7-3). analytics.py가 UPSERT.
CREATE TABLE IF NOT EXISTS `ans_weekly_price_stats` (
    `id`           BIGINT       NOT NULL AUTO_INCREMENT,
    `product_id`   BIGINT       NOT NULL,
    `week_start`   DATE         NOT NULL,
    `min_price`    BIGINT       NOT NULL,
    `max_price`    BIGINT       NOT NULL,
    `avg_price`    DECIMAL(12,2) NOT NULL,
    `record_count` BIGINT       NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_weekly_price_stats` (`product_id`, `week_start`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 상품 전기간 통계. analytics.py가 UPSERT (GROUP BY product_id). detect가 읽음.
CREATE TABLE IF NOT EXISTS `ans_product_stats` (
    `product_id`       BIGINT       NOT NULL,                  -- 자연키=대리키(PK), FK → stg_products
    `avg_price`        DECIMAL(12,2) NOT NULL,
    `min_price_ever`   BIGINT       NOT NULL,
    `max_price_ever`   BIGINT       NOT NULL,
    `first_crawled_at` DATETIME     NOT NULL,
    `last_crawled_at`  DATETIME     NOT NULL,
    `total_records`    BIGINT       NOT NULL,
    `updated_at`       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`product_id`),
    CONSTRAINT `fk_product_stats_product`
        FOREIGN KEY (`product_id`) REFERENCES `stg_products` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- ─────────────────────────────────────────────────────
--  VIEW: stg_latest_prices (마지막 — price_history 의존)
--  QUALIFY ROW_NUMBER() → 서브쿼리 + ROW_NUMBER() 필터 재작성(§6.7)
-- ─────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `stg_latest_prices` AS
SELECT `product_id`, `price`, `crawled_at`
FROM (
    SELECT
        `product_id`, `price`, `crawled_at`,
        ROW_NUMBER() OVER (PARTITION BY `product_id` ORDER BY `crawled_at` DESC) AS `rn`
    FROM `stg_price_history`
) `ranked`
WHERE `rn` = 1;
