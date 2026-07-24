-- =====================================================
-- 010_drop_products_brand_model.sql
-- 목적: STAGING.PRODUCTS의 미사용 컬럼 제거
--   - BRAND: 전 구간 NULL (대시보드 "브랜드" 열이 항상 "-"였음), 채울 가치 없어 제거
--   - MODEL_NUMBER: 002 DDL 정의에만 있었고 실제 테이블엔 부재 (IF EXISTS로 멱등 처리)
-- 주의: RAW.CRAWLED_PRICES.BRAND, WATCHLIST.BRAND는 워치리스트용으로 유지
-- =====================================================

USE DATABASE COMPUTER_PRICE;
USE SCHEMA STAGING;

ALTER TABLE PRODUCTS DROP COLUMN IF EXISTS BRAND;
ALTER TABLE PRODUCTS DROP COLUMN IF EXISTS MODEL_NUMBER;
