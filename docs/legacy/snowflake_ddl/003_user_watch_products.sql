-- 사용자 크롤링 대상 제품 관리 테이블
-- 대시보드에서 추가/삭제, DanawaCrawler가 크롤링 시 참조

USE DATABASE COMPUTER_PRICE;
USE SCHEMA STAGING;

CREATE TABLE IF NOT EXISTS WATCHLIST (
    ID           NUMBER       AUTOINCREMENT PRIMARY KEY,
    QUERY        VARCHAR(500) NOT NULL,             -- 검색어 (예: "라이젠 7800X3D")
    PCODE        VARCHAR(50)  NOT NULL UNIQUE,      -- 다나와 pcode (중복 추가 방지)
    PRODUCT_NAME VARCHAR(500),                      -- 실제 상품명 (검색 시 자동 입력)
    CATEGORY     VARCHAR(100) NOT NULL,             -- CPU / GPU / RAM / SSD / 기타
    BRAND        VARCHAR(200),                      -- 제조사 (선택)
    IS_ACTIVE    BOOLEAN      NOT NULL DEFAULT TRUE,
    ADDED_AT     TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- 기존 하드코딩 4개 제품 초기 데이터 (멱등성 보장 — pcode 중복 시 SKIP)
MERGE INTO WATCHLIST t
USING (
    SELECT col1 AS QUERY, col2 AS PCODE, col3 AS CATEGORY, col4 AS BRAND
    FROM VALUES
        ('라이젠 7800X3D', '19627934', 'CPU',  'AMD'),
        ('RTX 5070',      '77379452', 'GPU',  'NVIDIA'),
        ('RTX 5070 Ti',   '76464143', 'GPU',  'NVIDIA'),
        ('RX 9070 XT',    '77381483', 'GPU',  'AMD')
    AS seed(col1, col2, col3, col4)
) s ON t.PCODE = s.PCODE
WHEN NOT MATCHED THEN
    INSERT (QUERY, PCODE, CATEGORY, BRAND)
    VALUES (s.QUERY, s.PCODE, s.CATEGORY, s.BRAND);
