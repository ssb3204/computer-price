-- =====================================================
-- 005_pipeline_runs.sql
-- 목적: 파이프라인 실행 이력 추적 테이블 생성
--       전체 실행 1건(PIPELINE_RUNS) + 단계별 기록(PIPELINE_STEP_RUNS)
-- 스키마: RAW (원시 운영 데이터)
-- =====================================================

USE DATABASE COMPUTER_PRICE;
USE SCHEMA RAW;

-- =====================================================
-- 전체 파이프라인 실행 요약 (1회 실행당 1행)
-- RUN_ID 형식: run_YYYYMMDD_HHMMSS (예: run_20260406_210023)
-- =====================================================
CREATE TABLE IF NOT EXISTS PIPELINE_RUNS (
    RUN_ID       VARCHAR(30)    NOT NULL PRIMARY KEY,
    STARTED_AT   TIMESTAMP_NTZ NOT NULL,
    FINISHED_AT  TIMESTAMP_NTZ,
    DURATION_SEC FLOAT,
    STATUS       VARCHAR(10)    NOT NULL,  -- RUNNING / SUCCESS / PARTIAL / FAILED
    ERROR_MSG    VARCHAR(2000)
);

-- =====================================================
-- 파이프라인 단계별 실행 기록 (단계당 1행)
-- STEP_NAME: crawl / load_raw / transform / quality / detect / slack / analytics
-- =====================================================
CREATE TABLE IF NOT EXISTS PIPELINE_STEP_RUNS (
    RUN_ID       VARCHAR(30)    NOT NULL REFERENCES PIPELINE_RUNS(RUN_ID),
    STEP_NAME    VARCHAR(50)    NOT NULL,
    STARTED_AT   TIMESTAMP_NTZ NOT NULL,
    FINISHED_AT  TIMESTAMP_NTZ,
    DURATION_SEC FLOAT,
    RECORD_COUNT INT,
    STATUS       VARCHAR(10)    NOT NULL,  -- SUCCESS / FAILED / SKIPPED
    ERROR_MSG    VARCHAR(2000),
    PRIMARY KEY (RUN_ID, STEP_NAME)
);
