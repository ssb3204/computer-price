-- =====================================================
--  006: Snowflake Stream 도입 — IS_PROCESSED 대체
--
--  변경 내용:
--    1. CRAWLED_PRICES에 Append-Only Stream 생성
--       → transform 스텝이 Stream에서 새 레코드만 소비
--       → IS_PROCESSED 플래그 방식 대체 (Kafka consumer offset 동작 재현)
--    2. TRANSFORM_FAILURES 테이블 생성
--       → 가격 파싱/이상치 실패 레코드 감사용 (IS_PROCESSED=FALSE 역할 대체)
--    3. IS_PROCESSED, PROCESSED_AT 컬럼 제거
--
--  실행 순서: 기존 테이블에 적용하는 migration DDL
--  멱등성: IF NOT EXISTS / IF EXISTS 사용
-- =====================================================

USE DATABASE COMPUTER_PRICE;
USE SCHEMA RAW;

-- ── 1. IS_PROCESSED, PROCESSED_AT 컬럼 제거 ──────────────────────────────
-- Stream이 consumer offset 역할을 대체하므로 더 이상 불필요
ALTER TABLE CRAWLED_PRICES DROP COLUMN IF EXISTS IS_PROCESSED;
ALTER TABLE CRAWLED_PRICES DROP COLUMN IF EXISTS PROCESSED_AT;


-- ── 2. Append-Only Stream 생성 ────────────────────────────────────────────
-- APPEND_ONLY=TRUE: CRAWLED_PRICES는 INSERT만 발생 (UPDATE/DELETE 없음)
--   → INSERT 이벤트만 추적해 불필요한 오버헤드 제거
-- Stream 생성 시점 이후 INSERT된 레코드만 추적됨
CREATE STREAM IF NOT EXISTS CRAWLED_PRICES_STREAM
    ON TABLE CRAWLED_PRICES
    APPEND_ONLY = TRUE;


-- ── 3. 변환 실패 감사 테이블 ──────────────────────────────────────────────
-- IS_PROCESSED=FALSE로 레코드가 쌓이던 문제를 대체:
--   - 실패 레코드를 Stream에서 소비(consume)한 뒤 여기에 기록
--   - 이후 파이프라인 실행에서 실패 레코드를 재조회하지 않음
--   - REJECT_REASON으로 실패 원인 명시 (사후 분석 가능)
CREATE TABLE IF NOT EXISTS TRANSFORM_FAILURES (
    ID                NUMBER       AUTOINCREMENT PRIMARY KEY,
    CRAWLED_PRICES_ID NUMBER,                      -- RAW.CRAWLED_PRICES.ID 참조 (FK 아님)
    SITE              VARCHAR(50),
    CATEGORY          VARCHAR(100),
    PRODUCT_NAME      VARCHAR(2000),
    PRICE_TEXT        VARCHAR(100),
    CRAWLED_AT        TIMESTAMP_NTZ,
    REJECT_REASON     VARCHAR(200)  NOT NULL,
    FAILED_AT         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
