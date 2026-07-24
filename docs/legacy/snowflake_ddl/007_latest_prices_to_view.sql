-- =====================================================
--  007: STAGING.LATEST_PRICES 테이블 → VIEW 전환
--
--  변경 내용:
--    1. STAGING.LATEST_PRICES 테이블 삭제
--    2. 동일한 이름으로 VIEW 재생성
--       → PRICE_HISTORY에서 QUALIFY ROW_NUMBER()로 최신가 도출
--       → transform 스텝의 MERGE INTO LATEST_PRICES 불필요 → 제거
--
--  배경:
--    LATEST_PRICES 테이블은 PRICE_HISTORY의 파생값이라
--    매 파이프라인 실행마다 MERGE로 동기화해야 하는 이중 관리 문제 존재.
--    VIEW로 전환하면 항상 최신 상태가 보장되고 코드 단순화 가능.
--
--  멱등성: DROP IF EXISTS / CREATE OR REPLACE 사용
-- =====================================================

USE DATABASE COMPUTER_PRICE;
USE SCHEMA STAGING;

-- ── 1. 기존 테이블 삭제 ───────────────────────────────────────────────────
DROP TABLE IF EXISTS LATEST_PRICES;


-- ── 2. VIEW 생성 ─────────────────────────────────────────────────────────
-- PRICE_HISTORY에서 상품별 가장 최근 레코드(CRAWLED_AT 기준)를 반환
CREATE OR REPLACE VIEW LATEST_PRICES AS
SELECT
    PRODUCT_ID,
    PRICE,
    CRAWLED_AT
FROM STAGING.PRICE_HISTORY
QUALIFY ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY CRAWLED_AT DESC) = 1;
