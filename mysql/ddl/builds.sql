-- =============================================================
-- builds / build_items 테이블 (부품 조합 — 공개 게시물)
-- =============================================================
-- 설계 결정:
--   - 조합은 "누구나 볼 수 있는 게시물"이다. 읽기에는 소유자 제한이 없고,
--     생성/수정/삭제만 작성자(user_id)로 제한한다.
--   - 부품은 stg_watchlist(전역 크롤링 대상 마스터)를 참조한다. 사용자가
--     자기 워치리스트에서 골라 담지만, 저장되는 건 전역 상품이라
--     다른 사람이 그 조합을 열어도 부품 정보가 그대로 보인다.
--   - BUILD_PRICE_HISTORY(총액 이력 실체 테이블)는 두지 않는다.
--     ans_ 통계 테이블 재설계(2026-07-24)와 같은 판단 — 일별 1점 기준
--     연 수천 행 규모라 stg_price_history 에서 즉석 집계로 충분하다.
--     느려지면 그때 실체화한다(재도입 기준: 화면에 그리는 점 10만 개).
--   - 수량(RAM 2개 등)은 넣지 않는다. 필요해지면 build_items.quantity
--     컬럼 하나 추가로 끝난다(YAGNI).
--   - 카테고리 중복은 막지 않는다. RAM 2개, SSD 2개 조합이 자연스럽다.
--
-- 크롤링 유지와의 관계 (중요):
--   stg_watchlist.is_active 를 0으로 내리는 조건에 build_items 도 포함해야 한다.
--   user_watchlist 만 세면, 조합에 담긴 상품을 작성자가 워치리스트에서 빼는
--   순간 크롤링이 멈춰 공개 조합의 가격이 갱신되지 않는다.
--   → watchlist_repo.deactivate_if_orphaned 가 두 테이블을 모두 확인한다.
-- =============================================================

CREATE TABLE IF NOT EXISTS `builds` (
    `id`         BIGINT       NOT NULL AUTO_INCREMENT,
    `user_id`    BIGINT       NOT NULL,                    -- 작성자
    `name`       VARCHAR(100) NOT NULL,                    -- "게이밍용", "사무용"
    `created_at` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                              ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_builds_user_name` (`user_id`, `name`),  -- 한 사람이 같은 이름 두 번 금지
    KEY `idx_builds_created_at` (`created_at`),            -- 최신순 목록 조회용
    CONSTRAINT `fk_builds_user`
        FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE IF NOT EXISTS `build_items` (
    `id`           BIGINT   NOT NULL AUTO_INCREMENT,
    `build_id`     BIGINT   NOT NULL,
    `watchlist_id` BIGINT   NOT NULL,                      -- stg_watchlist(전역 상품)
    `added_at`     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_build_items` (`build_id`, `watchlist_id`),
    KEY `idx_build_items_watchlist` (`watchlist_id`),      -- 크롤링 유지 판정 조회용
    CONSTRAINT `fk_build_items_build`
        FOREIGN KEY (`build_id`) REFERENCES `builds` (`id`) ON DELETE CASCADE,
    CONSTRAINT `fk_build_items_watchlist`
        FOREIGN KEY (`watchlist_id`) REFERENCES `stg_watchlist` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
