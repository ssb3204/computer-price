"""FastAPI 앱 진입점.

실행:
    uvicorn src.api.main:app --reload
문서:
    http://127.0.0.1:8000/docs  (Swagger, 자동 생성)
"""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from src.api.build_router import public_build_router, user_build_router
from src.api.users_router import router as users_router
from src.api.watchlist_router import crawl_router, user_watchlist_router

app = FastAPI(title="computer_price API", version="0.1.0")

app.include_router(users_router)
app.include_router(crawl_router)
app.include_router(user_watchlist_router)
app.include_router(public_build_router)
app.include_router(user_build_router)

_STATIC_DIR = Path(__file__).parent / "static"

# vendor/chart.umd.min.js 같은 정적 자산을 그대로 서빙한다.
# 차트 라이브러리는 CDN 이 아니라 저장소에 넣어 두고 여기서 내려준다
# (오프라인·로컬 Docker 에서도 뜨고, 외부를 부르지 않는 기존 페이지 성격 유지).
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


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


@app.get("/builds", tags=["meta"])
def builds_page() -> FileResponse:
    """부품 조합 페이지. 공개 조합 API 는 경로 충돌을 피해 /api/builds 에 있다."""
    return FileResponse(_STATIC_DIR / "builds.html")
