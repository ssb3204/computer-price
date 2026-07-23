-- =============================================================
-- user_watchlist 테이블 (사용자별 워치리스트 — 연결 테이블)
-- =============================================================
-- 설계 결정:
--   - stg_watchlist(전역, pcode UNIQUE — 실제 크롤링 대상 마스터)와 users 를
--     연결하는 다대다(N:M) 연결 테이블. 상품 1개를 여러 사용자가 동시에
--     담을 수 있어 컬럼 하나로는 "소유자"를 표현할 수 없기 때문(pcode UNIQUE와 충돌).
--   - 화면(내 워치리스트 조회)은 이 테이블을 user_id 로 조회한다.
--     stg_watchlist.is_active 는 이 조회에 관여하지 않는다(별개 관심사).
--   - stg_watchlist.is_active 는 "이 상품을 참조하는 user_watchlist row 가
--     0개가 됐을 때만" 0으로 내린다(마지막 사용자가 뺐을 때). 즉 한 명이라도
--     담고 있으면 계속 크롤링되고, 그 사람 화면에도 계속 보인다.
--   - 대리키 id + 자연키(user_id, watchlist_id) UNIQUE 컨벤션 동일 적용.
-- =============================================================

CREATE TABLE IF NOT EXISTS `user_watchlist` (
    `id`           BIGINT   NOT NULL AUTO_INCREMENT,
    `user_id`      BIGINT   NOT NULL,
    `watchlist_id` BIGINT   NOT NULL,
    `added_at`     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_user_watchlist` (`user_id`, `watchlist_id`),
    CONSTRAINT `fk_user_watchlist_user`
        FOREIGN KEY (`user_id`) REFERENCES `users` (`id`),
    CONSTRAINT `fk_user_watchlist_watchlist`
        FOREIGN KEY (`watchlist_id`) REFERENCES `stg_watchlist` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
