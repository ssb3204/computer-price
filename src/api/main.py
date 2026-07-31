"""FastAPI 앱 진입점.

실행:
    uvicorn src.api.main:app --reload
문서:
    http://127.0.0.1:8000/docs  (Swagger, 자동 생성)
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, RedirectResponse

from src.api.build_router import public_build_router, user_build_router
from src.api.users_router import router as users_router
from src.api.watchlist_router import crawl_router, user_watchlist_router
from src.common.config import MySQLSettings
from src.common.mysql_client import init_pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    # API 프로세스에서만 커넥션 풀을 켠다.
    # 크롤러/파이프라인은 init_pool()을 호출하지 않으므로 기존 동작(매번 새 커넥션)을 유지한다.
    init_pool(MySQLSettings())
    yield


app = FastAPI(title="computer_price API", version="0.1.0", lifespan=lifespan)

app.include_router(users_router)
app.include_router(crawl_router)
app.include_router(user_watchlist_router)
app.include_router(public_build_router)
app.include_router(user_build_router)

_STATIC_DIR = Path(__file__).parent / "static"


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", tags=["meta"])
def root() -> RedirectResponse:
    return RedirectResponse(url="/main")


@app.get("/main", tags=["meta"])
def main_page() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")


@app.get("/signup", tags=["meta"])
def signup_page() -> FileResponse:
    return FileResponse(_STATIC_DIR / "signup.html")


@app.get("/home", tags=["meta"])
def home_page() -> FileResponse:
    return FileResponse(_STATIC_DIR / "home.html")


@app.get("/mypage", tags=["meta"])
def mypage_page() -> FileResponse:
    return FileResponse(_STATIC_DIR / "mypage.html")


@app.get("/watchlist", tags=["meta"])
def watchlist_page() -> FileResponse:
    return FileResponse(_STATIC_DIR / "watchlist.html")
