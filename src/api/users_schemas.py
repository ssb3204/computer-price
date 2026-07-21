"""users API 요청/응답 스키마 (Pydantic).

응답에는 절대 password_hash 를 포함하지 않는다.
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class UserCreate(BaseModel):
    """회원가입 요청."""

    username: str = Field(min_length=3, max_length=50)
    password: str = Field(min_length=8, max_length=128)


class PasswordUpdate(BaseModel):
    """비밀번호 변경 요청."""

    new_password: str = Field(min_length=8, max_length=128)


class UserPublic(BaseModel):
    """외부로 노출하는 유저 정보 (password_hash 제외)."""

    id: int
    username: str
    created_at: datetime
    deleted_at: datetime | None = None
