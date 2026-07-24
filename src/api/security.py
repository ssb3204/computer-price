"""비밀번호 해싱 유틸 (bcrypt + SHA-256 프리해싱).

passlib 은 최신 bcrypt(4.x/5.x)와 호환 문제가 잦고 사실상 유지보수가
멈춰서, bcrypt 라이브러리를 직접 사용한다.

── bcrypt 72바이트 문제와 그 해결 ──────────────────────────────
bcrypt 는 입력의 앞 72바이트까지만 사용하고 초과분을 조용히 버린다.
그래서 앞 72바이트가 같으면 뒤가 달라도 같은 비밀번호로 취급되는
심각한 문제가 생긴다(길이 제한만으로는 이걸 문자 수/바이트 수
불일치 때문에 확실히 막기 어렵다).

표준 해결책: 비밀번호를 먼저 SHA-256 으로 해시해 "고정 길이 다이제스트"로
만든 뒤 그 결과를 bcrypt 에 넣는다. SHA-256 출력은 항상 32바이트이고,
이를 base64 로 인코딩해도 44바이트라 절대 72바이트를 넘지 않는다.
따라서 원본 비밀번호가 아무리 길어도 truncation 이 발생하지 않으며,
전체 입력이 bcrypt 검증에 반영된다.

주의: SHA-256 결과를 hex/base64 문자열로 만들면 그 안에 NUL 바이트가
없으므로, bcrypt 의 "NUL 바이트에서 잘림" 문제도 함께 회피된다.
"""
from __future__ import annotations

import base64
import hashlib

import bcrypt


def _prehash(plain_password: str) -> bytes:
    """비밀번호를 SHA-256 → base64 로 고정 길이(44바이트) 다이제스트화.

    bcrypt 의 72바이트 truncation 및 NUL 바이트 절단 문제를 원천 차단한다.
    """
    digest = hashlib.sha256(plain_password.encode("utf-8")).digest()
    return base64.b64encode(digest)  # 44바이트, NUL 없음


def hash_password(plain_password: str) -> str:
    """평문 비밀번호를 bcrypt 해시 문자열로 변환."""
    hashed = bcrypt.hashpw(_prehash(plain_password), bcrypt.gensalt())
    return hashed.decode("utf-8")


def verify_password(plain_password: str, password_hash: str) -> bool:
    """평문 비밀번호가 저장된 해시와 일치하는지 검증."""
    try:
        return bcrypt.checkpw(
            _prehash(plain_password), password_hash.encode("utf-8")
        )
    except ValueError:
        # 저장된 해시 형식이 잘못된 경우 등
        return False
