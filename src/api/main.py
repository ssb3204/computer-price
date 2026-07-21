"""FastAPI 앱 진입점.

실행:
    uvicorn src.api.main:app --reload
문서:
    http://127.0.0.1:8000/docs  (Swagger, 자동 생성)
"""
from __future__ import annotations

from fastapi import FastAPI

from src.api.users_router import router as users_router

app = FastAPI(title="computer_price API", version="0.1.0")

app.include_router(users_router)


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}
