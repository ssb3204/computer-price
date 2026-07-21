"""users CRUD 라우터 (FastAPI).

엔드포인트:
  POST   /users            회원가입 (신규 or 탈퇴계정 재활성화)
  GET    /users/{user_id}  단건 조회 (활성 유저만)
  PATCH  /users/{user_id}  비밀번호 변경
  DELETE /users/{user_id}  탈퇴 (soft delete)

인증(로그인 세션/토큰)은 이번 범위에 포함하지 않는다. 순수 CRUD 만 제공한다.
따라서 지금은 어떤 요청이든 user_id 로 대상을 직접 지정한다.
(추후 인증 도입 시 "본인만 수정/삭제 가능" 가드를 여기에 추가.)
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from pymysql.err import IntegrityError

from src.api import users_repo
from src.api.security import hash_password
from src.api.users_schemas import PasswordUpdate, UserCreate, UserPublic
from src.common.config import MySQLSettings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/users", tags=["users"])


def get_settings() -> MySQLSettings:
    """설정 의존성. 실제 config 초기화 방식에 맞춰 조정 가능.

    pydantic-settings 기반이면 MySQLSettings() 가 환경변수/‑.env 에서
    값을 읽어온다. 초기화 인자가 필요하면 이 함수만 수정하면 된다.
    """
    return MySQLSettings()


def _to_public(user: users_repo.UserRow) -> UserPublic:
    return UserPublic(
        id=user.id,
        username=user.username,
        created_at=user.created_at,
        deleted_at=user.deleted_at,
    )


@router.post("", response_model=UserPublic, status_code=status.HTTP_201_CREATED)
def create_user(
    payload: UserCreate,
    settings: MySQLSettings = Depends(get_settings),
) -> UserPublic:
    """회원가입.

    username 상태에 따라 3-way 분기:
      1) 존재 + 활성      -> 409 중복
      2) 존재 + 탈퇴상태   -> 재활성화(본인 복귀로 간주, 비밀번호 재설정)
      3) 미존재           -> 신규 INSERT

    주의: autocommit=True 라 트랜잭션 잠금이 없으므로, 조회~삽입 사이에
    다른 요청이 같은 username 을 넣는 경쟁이 이론상 가능하다. 그래서
    신규 INSERT 는 username UNIQUE 제약을 최종 방어선으로 삼고,
    IntegrityError 를 409 로 변환한다(check-then-act 의 원자성 보완).
    """
    pw_hash = hash_password(payload.password)
    existing = users_repo.get_by_username(settings, payload.username)

    # 1) 활성 유저가 이미 있으면 중복
    if existing is not None and existing.deleted_at is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="이미 사용 중인 아이디입니다.",
        )

    # 2) 탈퇴한 유저면 재활성화 (같은 id 를 되살림)
    if existing is not None and existing.deleted_at is not None:
        users_repo.reactivate_user(settings, existing.id, pw_hash)
        refreshed = users_repo.get_active_by_id(settings, existing.id)
        # 재활성화 직후엔 반드시 활성 상태여야 한다.
        assert refreshed is not None
        return _to_public(refreshed)

    # 3) 신규 INSERT (UNIQUE 제약을 최종 방어선으로)
    try:
        new_id = users_repo.insert_user(settings, payload.username, pw_hash)
    except IntegrityError:
        # 조회~삽입 사이에 동일 username 이 선점된 경쟁 상황
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="이미 사용 중인 아이디입니다.",
        )
    created = users_repo.get_active_by_id(settings, new_id)
    assert created is not None
    return _to_public(created)


@router.get("/{user_id}", response_model=UserPublic)
def read_user(
    user_id: int,
    settings: MySQLSettings = Depends(get_settings),
) -> UserPublic:
    """활성 유저 단건 조회. 탈퇴했거나 없으면 404."""
    user = users_repo.get_active_by_id(settings, user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="사용자를 찾을 수 없습니다.",
        )
    return _to_public(user)


@router.patch("/{user_id}", response_model=UserPublic)
def update_user_password(
    user_id: int,
    payload: PasswordUpdate,
    settings: MySQLSettings = Depends(get_settings),
) -> UserPublic:
    """활성 유저의 비밀번호 변경."""
    pw_hash = hash_password(payload.new_password)
    affected = users_repo.update_password(settings, user_id, pw_hash)
    if affected == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="사용자를 찾을 수 없습니다.",
        )
    user = users_repo.get_active_by_id(settings, user_id)
    assert user is not None
    return _to_public(user)


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(
    user_id: int,
    settings: MySQLSettings = Depends(get_settings),
) -> None:
    """탈퇴 (soft delete). deleted_at 에 UTC 시각 기록."""
    affected = users_repo.soft_delete_user(settings, user_id)
    if affected == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="사용자를 찾을 수 없습니다.",
        )
    return None
