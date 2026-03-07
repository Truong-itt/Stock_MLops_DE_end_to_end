import asyncio
import logging
import os
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response

from database import db
from routes import router
from ws import websocket_endpoint, poll_latest_prices

# ─── Logging ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("backend")

poll_task = None
STATIC_DIR = Path(os.getenv("STATIC_DIR", "/app/static"))


# ─── Lifespan ───────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global poll_task
    logger.info("Starting backend ...")
    db.connect()
    poll_task = asyncio.create_task(poll_latest_prices())
    logger.info("Background poller started")
    yield
    logger.info("Shutting down ...")
    if poll_task:
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass
    db.close()


# ─── App ────────────────────────────────────────────────────────────
app = FastAPI(title="Stock Real-Time Dashboard", version="2.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# REST API
app.include_router(router)

# WebSocket
app.websocket("/ws")(websocket_endpoint)

# Frontend static files
if STATIC_DIR.is_dir():
    for sub in ("css", "js", "img"):
        p = STATIC_DIR / sub
        if p.is_dir():
            app.mount(f"/{sub}", StaticFiles(directory=str(p)), name=sub)

    @app.get("/")
    async def serve_index():
        return FileResponse(str(STATIC_DIR / "index.html"))

    @app.get("/favicon.ico")
    async def favicon():
        svg = (
            '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" '
            'fill="none" stroke="%236366f1" stroke-width="2.2" '
            'stroke-linecap="round" stroke-linejoin="round">'
            '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>'
        )
        return Response(content=svg, media_type="image/svg+xml")

