-- =====================================================
--  009: 미사용 컬럼 제거 + WATCHLIST.SITE 한글 통일
--
--  변경 내용:
--    1. STAGING.PRODUCTS.MODEL_NUMBER 삭제 (미사용)
--    2. STAGING.PRICE_ALERTS.IS_READ 삭제 (읽음 처리 기능 미구현)
--    3. STAGING.WATCHLIST.SITE 값을 영문 코드 → 한글 표시명으로 통일
--       danawa → 다나와, compuzone → 컴퓨존, kjwwang → 견적왕
--       (PRODUCTS.SITE와 동일한 형식으로 맞춰 CASE 변환 제거)
--
--  멱등성: IF EXISTS / 조건부 UPDATE 사용
-- =====================================================

USE DATABASE COMPUTER_PRICE;

-- ── 1. 미사용 컬럼 제거 ────────────────────────────────────────────────────

USE SCHEMA STAGING;

ALTER TABLE PRODUCTS     DROP COLUMN IF EXISTS MODEL_NUMBER;
ALTER TABLE PRICE_ALERTS DROP COLUMN IF EXISTS IS_READ;

-- ── 2. WATCHLIST.SITE 한글 통일 ────────────────────────────────────────────

UPDATE STAGING.WATCHLIST
SET SITE = CASE SITE
    WHEN 'danawa'    THEN '다나와'
    WHEN 'compuzone' THEN '컴퓨존'
    WHEN 'kjwwang'   THEN '견적왕'
    ELSE SITE
END
WHERE SITE IN ('danawa', 'compuzone', 'kjwwang');

-- NOTE: ALTER COLUMN SET DEFAULT은 Snowflake 미지원.
-- 컬럼 DEFAULT는 'danawa'로 남아 있으나 add_watch_product()가 항상 site를 명시하므로 무관.
