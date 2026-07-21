"""users CRUD 라우터 (FastAPI).

엔드포인트:
  POST   /users            회원가입 (신규만; 탈퇴 아이디 재활성화는 미지원)
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

    pydantic-settings 기반이면 MySQLSettings() 가 환경변수/.env 에서
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

    username 재사용 정책 A: username UNIQUE 는 활성/탈퇴 무관하게 유지된다.
    따라서 이미 존재하는 username(활성이든 탈퇴든)은 모두 409 로 거절한다.

    탈퇴 계정 "재활성화"는 지금 단계에서는 지원하지 않는다. 이유:
    재활성화는 "정말 그 계정의 원래 주인인가"를 확인해야 안전한데,
    현재는 로그인 인증도 이메일 검증도 없어 소유권을 확인할 수단이 없다.
    확인 없이 재활성화하면 남이 탈퇴 계정의 아이디를 가로챌 수 있다.
    -> 인증 시스템이 도입된 뒤 "탈퇴 계정 복구" 기능으로 별도 설계한다.

    성능/DoS 주의: bcrypt 해싱은 비용이 크므로, 먼저 username 중복을
    확인해 거절될 요청은 해싱 없이 빠르게 반려한다(해싱을 뒤로 미룸).
    또한 autocommit=True 라 조회~삽입 사이 경쟁이 가능하므로,
    최종 방어선으로 username UNIQUE 제약 위반(IntegrityError)을 409 로 변환한다.
    """
    # 1) 먼저 중복 확인 (해싱 전에 — 거절될 요청의 해싱 비용 회피)
    #    활성/탈퇴 무관하게 username 이 존재하면 거절(정책 A).
    existing = users_repo.get_by_username(settings, payload.username)
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="이미 사용 중인 아이디입니다.",
        )

    # 2) 통과한 요청만 해싱 후 INSERT
    pw_hash = hash_password(payload.password)
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
    """활성 유저의 비밀번호 변경.

    존재하지 않는 user_id 는 해싱 전에 404 로 반려한다(불필요한 해싱 회피).
    """
    # 대상이 없으면 해싱 전에 404
    target = users_repo.get_active_by_id(settings, user_id)
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="사용자를 찾을 수 없습니다.",
        )

    pw_hash = hash_password(payload.new_password)
    users_repo.update_password(settings, user_id, pw_hash)
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