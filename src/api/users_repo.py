"""users 테이블 데이터 접근 계층 (repository).

기존 mysql_client.get_connection() 을 그대로 사용한다.
주의: 이 프로젝트의 커서는 DictCursor 가 아니라 기본 튜플 커서이므로,
      SELECT 결과를 row[0], row[1] ... 위치 인덱싱으로 다룬다.
      autocommit=True 이므로 각 execute 는 즉시 커밋된다(명시적 commit 불필요).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime

from src.common.config import MySQLSettings
from src.common.mysql_client import get_connection

logger = logging.getLogger(__name__)

# SELECT 시 항상 이 컬럼 순서를 사용한다(튜플 인덱싱 기준을 한 곳에 고정).
_USER_COLUMNS = "id, username, password_hash, email, name, nickname, birth_date, created_at, deleted_at"


@dataclass
class UserRow:
    """users row 를 튜플 대신 이름으로 다루기 위한 매핑 객체."""

    id: int
    username: str
    password_hash: str
    email: str
    name: str
    nickname: str
    birth_date: date
    created_at: datetime
    deleted_at: datetime | None

    @classmethod
    def from_tuple(cls, row: tuple) -> UserRow:
        # _USER_COLUMNS 순서와 반드시 일치해야 한다.
        return cls(
            id=row[0],
            username=row[1],
            password_hash=row[2],
            email=row[3],
            name=row[4],
            nickname=row[5],
            birth_date=row[6],
            created_at=row[7],
            deleted_at=row[8],
        )


def get_by_username(settings: MySQLSettings, username: str) -> UserRow | None:
    """username 으로 조회. 활성/탈퇴 무관하게 존재하는 row 를 반환(없으면 None).

    재가입/재활성화 분기 판단을 위해 탈퇴한 row 도 함께 조회한다.
    """
    sql = f"SELECT {_USER_COLUMNS} FROM users WHERE username = %s"
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (username,))
            row = cur.fetchone()
    return UserRow.from_tuple(row) if row else None


def get_by_email(settings: MySQLSettings, email: str) -> UserRow | None:
    """email 로 조회. 활성/탈퇴 무관하게 존재하는 row 를 반환(없으면 None)."""
    sql = f"SELECT {_USER_COLUMNS} FROM users WHERE email = %s"
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (email,))
            row = cur.fetchone()
    return UserRow.from_tuple(row) if row else None


def get_by_nickname(settings: MySQLSettings, nickname: str) -> UserRow | None:
    """nickname 으로 조회. 활성/탈퇴 무관하게 존재하는 row 를 반환(없으면 None)."""
    sql = f"SELECT {_USER_COLUMNS} FROM users WHERE nickname = %s"
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (nickname,))
            row = cur.fetchone()
    return UserRow.from_tuple(row) if row else None


def get_active_by_id(settings: MySQLSettings, user_id: int) -> UserRow | None:
    """id 로 활성 유저(deleted_at IS NULL)만 조회."""
    sql = (
        f"SELECT {_USER_COLUMNS} FROM users "
        f"WHERE id = %s AND deleted_at IS NULL"
    )
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id,))
            row = cur.fetchone()
    return UserRow.from_tuple(row) if row else None


def insert_user(
    settings: MySQLSettings,
    username: str,
    password_hash: str,
    email: str,
    name: str,
    nickname: str,
    birth_date: date,
) -> int:
    """신규 유저 INSERT. 새 id 를 반환.

    username/email/nickname UNIQUE 제약에 걸리면 pymysql 이 IntegrityError 를
    던진다(호출부에서 처리).
    """
    sql = (
        "INSERT INTO users (username, password_hash, email, name, nickname, birth_date) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (username, password_hash, email, name, nickname, birth_date))
            new_id = cur.lastrowid
    return new_id


def reactivate_user(
    settings: MySQLSettings, user_id: int, password_hash: str
) -> None:
    """탈퇴한 유저(deleted_at IS NOT NULL)를 재활성화.

    deleted_at 을 NULL 로 되돌리고 비밀번호를 새로 설정한다.

    ⚠️ 현재 미사용: 소유권 검증 수단(로그인 인증/이메일)이 없어 재활성화를
    막아둔 상태다(users_router.create_user 참고). 인증 도입 후 "탈퇴 계정
    복구" 기능에서 소유권 확인과 함께 사용하기 위해 남겨둔다.
    """
    sql = (
        "UPDATE users SET password_hash = %s, deleted_at = NULL "
        "WHERE id = %s AND deleted_at IS NOT NULL"
    )
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (password_hash, user_id))


def update_password(
    settings: MySQLSettings, user_id: int, password_hash: str
) -> int:
    """활성 유저의 비밀번호 변경. 영향받은 row 수를 반환(0이면 대상 없음)."""
    sql = (
        "UPDATE users SET password_hash = %s "
        "WHERE id = %s AND deleted_at IS NULL"
    )
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            affected = cur.execute(sql, (password_hash, user_id))
    return affected


def update_profile(
    settings: MySQLSettings, user_id: int, name: str, birth_date: date
) -> int:
    """활성 유저의 이름/생년월일 변경. 영향받은 row 수를 반환(0이면 대상 없음)."""
    sql = (
        "UPDATE users SET name = %s, birth_date = %s "
        "WHERE id = %s AND deleted_at IS NULL"
    )
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            affected = cur.execute(sql, (name, birth_date, user_id))
    return affected


def soft_delete_user(settings: MySQLSettings, user_id: int) -> int:
    """활성 유저를 soft delete. deleted_at 에 현재 UTC 시각을 기록.

    영향받은 row 수를 반환(0이면 이미 탈퇴했거나 존재하지 않음).
    커넥션이 UTC 로 고정돼 있으므로 UTC_TIMESTAMP() 사용.
    """
    sql = (
        "UPDATE users SET deleted_at = UTC_TIMESTAMP() "
        "WHERE id = %s AND deleted_at IS NULL"
    )
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            affected = cur.execute(sql, (user_id,))
    return affected
