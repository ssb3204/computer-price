"""users API 요청/응답 스키마 (Pydantic).

응답에는 절대 password_hash 를 포함하지 않는다.
"""
from __future__ import annotations

import re
from datetime import date, datetime

from pydantic import BaseModel, Field, field_validator

# email-validator 의존성 추가 없이 형식만 가볍게 검증 (RFC 전체 검증은 목적이 아님)
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class UserCreate(BaseModel):
    """회원가입 요청."""

    username: str = Field(min_length=3, max_length=50)
    password: str = Field(min_length=8, max_length=128)
    email: str = Field(max_length=255)
    name: str = Field(min_length=1, max_length=100)
    nickname: str = Field(min_length=1, max_length=50)
    birth_date: date

    @field_validator("email")
    @classmethod
    def _validate_email(cls, v: str) -> str:
        if not _EMAIL_RE.match(v):
            raise ValueError("올바른 이메일 형식이 아닙니다.")
        return v


class UserLogin(BaseModel):
    """로그인 요청. 저장된 자격증명과 비교하는 용도라 회원가입과 달리 길이 제약을 두지 않는다."""

    username: str
    password: str


class PasswordUpdate(BaseModel):
    """비밀번호 변경 요청."""

    new_password: str = Field(min_length=8, max_length=128)


class PasswordVerify(BaseModel):
    """본인 확인용 비밀번호 재입력 요청 (마이페이지 수정/탈퇴 진입 전 게이트)."""

    password: str


class ProfileUpdate(BaseModel):
    """마이페이지 프로필 수정 요청. username/email/nickname 처럼 UNIQUE 제약이 걸린
    필드는 여기 포함하지 않는다(중복 확인 UX가 별도로 필요해 이번 범위에서 제외)."""

    name: str = Field(min_length=1, max_length=100)
    birth_date: date


class UserPublic(BaseModel):
    """외부로 노출하는 유저 정보 (password_hash 제외)."""

    id: int
    username: str
    email: str
    name: str
    nickname: str
    birth_date: date
    created_at: datetime
    deleted_at: datetime | None = None
