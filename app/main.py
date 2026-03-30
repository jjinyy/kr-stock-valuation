from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.api import router as api_router
from app.services.scheduler import start_scheduler

_WEB_DIR = Path(__file__).resolve().parent / "web"
_DIST = _WEB_DIR / "dist"

_NO_BUILD_HTML = """<!doctype html>
<html lang="ko"><head><meta charset="utf-8"/><title>kr-analyze — 빌드 필요</title></head>
<body style="font-family:system-ui,sans-serif;max-width:560px;margin:48px auto;padding:0 16px;">
  <h1>프론트 빌드가 없습니다</h1>
  <p>React UI는 <code>app/web/dist/</code>에 빌드된 파일이 있어야 합니다.</p>
  <pre style="background:#f4f4f4;padding:12px;border-radius:8px;">cd frontend
npm install
npm run build</pre>
  <p>이후 API는 그대로 <code>/api</code> 에서 사용할 수 있습니다.</p>
</body></html>"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = start_scheduler()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)


app = FastAPI(title="kr-analyze", lifespan=lifespan)
app.include_router(api_router)


@app.get("/health")
def health():
    return {"ok": True}


if _DIST.is_dir() and (_DIST / "index.html").is_file():
    app.mount("/", StaticFiles(directory=str(_DIST), html=True), name="spa")
else:

    @app.get("/")
    def home():
        return HTMLResponse(_NO_BUILD_HTML)
