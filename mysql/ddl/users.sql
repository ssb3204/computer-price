-- =============================================================
-- users 테이블 (회원)
-- =============================================================
-- 설계 결정:
--   - 대리키 id (BIGINT AUTO_INCREMENT), 자연키 username UNIQUE
--     -> 기존 stg_/ans_ 테이블의 "대리키 + 자연키 UNIQUE" 컨벤션과 동일
--   - 비밀번호는 bcrypt 해시만 저장 (평문 저장 금지). bcrypt 해시는 60자이나
--     알고리즘 변경 여유를 위해 VARCHAR(255)
--   - soft delete: deleted_at 이 NULL 이면 활성, 값이 있으면 탈퇴 시각
--   - username 재사용 정책 A: 순수 UNIQUE 유지 -> 탈퇴해도 타인이 재사용 불가.
--     단, 본인 복귀 시 같은 row 를 재활성화(deleted_at = NULL)하여 재사용 가능
--   - 모든 시각은 UTC (커넥션이 SET time_zone='+00:00' 으로 UTC 고정)
--     -> DEFAULT CURRENT_TIMESTAMP 도 UTC 로 기록됨
--   - email/nickname 은 username 과 동일한 정책(활성/탈퇴 무관 UNIQUE)을 적용한다.
--     회원가입 시점에는 앱(Pydantic)이 필수값으로 강제하지만, 컬럼 자체는
--     NULL 허용이다 — 이 컬럼 추가 이전에 가입한 기존 row 를 깨지 않기 위함
--     (MySQL UNIQUE 는 NULL 을 중복으로 취급하지 않으므로 다중 NULL 허용됨).
-- =============================================================

CREATE TABLE IF NOT EXISTS `users` (
    `id`            BIGINT       NOT NULL AUTO_INCREMENT,
    `username`      VARCHAR(50)  NOT NULL,
    `password_hash` VARCHAR(255) NOT NULL,
    `email`         VARCHAR(255) NULL,
    `name`          VARCHAR(100) NULL,
    `nickname`      VARCHAR(50)  NULL,
    `birth_date`    DATE         NULL,
    `created_at`    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `deleted_at`    DATETIME     NULL     DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_users_username` (`username`),
    UNIQUE KEY `uq_users_email` (`email`),
    UNIQUE KEY `uq_users_nickname` (`nickname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
