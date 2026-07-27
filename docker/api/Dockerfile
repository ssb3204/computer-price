# FastAPI (users API) 컨테이너
# PyMySQL 은 순수 파이썬이라 C 컴파일러(gcc)나 libpq-dev 가 필요 없어 제외했다.
FROM python:3.11-slim

WORKDIR /app

# 의존성만 먼저 설치해 레이어 캐싱 활용 (소스가 바뀌어도 의존성 재설치 안 함)
COPY pyproject.toml .
RUN pip install --no-cache-dir .

# 소스 복사 (compose 에서 볼륨으로 덮어쓰지만, 볼륨 없이 단독 실행도 가능하게)
COPY src/ src/

# FastAPI 기본 포트
EXPOSE 8000

# uvicorn 으로 실행. 컨테이너 밖에서 접근하려면 host 0.0.0.0 필수
# (127.0.0.1 로 바인딩하면 컨테이너 안에서만 접속 가능해 외부에서 못 붙음)
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
