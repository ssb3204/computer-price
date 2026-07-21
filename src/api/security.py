"""비밀번호 해싱 유틸 (bcrypt 직접 사용).

passlib 은 최신 bcrypt(4.x)와 호환 문제가 잦고 사실상 유지보수가 멈춰서,
bcrypt 라이브러리를 직접 사용한다.

평문 비밀번호는 절대 저장하지 않는다. 저장 전 반드시 hash_password() 로
해시하고, 검증은 verify_password() 로 한다.

bcrypt 주의점: bcrypt 는 입력의 72바이트까지만 사용하고 초과분은 무시한다.
따라서 초과 입력이 조용히 잘려 서로 다른 비밀번호가 같게 취급되는 일을
막기 위해, 스키마 단에서 password 최대 길이를 제한한다(users_schemas 참고).
"""
from __future__ import annotations

import bcrypt

# bcrypt 가 실제로 사용하는 최대 바이트 수.
_BCRYPT_MAX_BYTES = 72


def _to_bcrypt_bytes(plain_password: str) -> bytes:
    """평문을 UTF-8 바이트로 변환. 72바이트 초과 시 명시적으로 자른다.

    (스키마에서 길이를 제한하지만, 방어적으로 여기서도 잘라 일관성을 보장.)
    """
    return plain_password.encode("utf-8")[:_BCRYPT_MAX_BYTES]


def hash_password(plain_password: str) -> str:
    """평문 비밀번호를 bcrypt 해시 문자열로 변환."""
    hashed = bcrypt.hashpw(_to_bcrypt_bytes(plain_password), bcrypt.gensalt())
    return hashed.decode("utf-8")


def verify_password(plain_password: str, password_hash: str) -> bool:
    """평문 비밀번호가 저장된 해시와 일치하는지 검증."""
    try:
        return bcrypt.checkpw(
            _to_bcrypt_bytes(plain_password), password_hash.encode("utf-8")
        )
    except ValueError:
        # 저장된 해시 형식이 잘못된 경우 등
        return False
