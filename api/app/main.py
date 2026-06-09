import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from app.config import settings
from app.database import Base, engine
from app.middleware.cors import add_cors
from app.middleware.logging import LoggingMiddleware
from app.middleware.rate_limit import limiter
from app.routers import auth, crimes, health, reports

logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Tables managed by Alembic (alembic upgrade head).
    # create_all kept as fallback for local dev without a migration run.
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="Crime Data API",
    description=(
        "Production-grade REST API for UK Police crime data. "
        "Features: JWT auth, rate limiting, Redis caching, async SQLAlchemy, "
        "AI-generated briefing reports, natural language RAG queries, "
        "cursor pagination, and background job processing."
    ),
    version=settings.app_version,
    lifespan=lifespan,
)

# ── Middleware (last added = first to run) ────────────────────────────────────
add_cors(app)
app.add_middleware(LoggingMiddleware)
app.add_middleware(SlowAPIMiddleware)

# ── Rate limiting ─────────────────────────────────────────────────────────────
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(health.router)                              # /health, /health/ready
app.include_router(auth.router, prefix=settings.api_prefix)   # /api/v1/auth/...
app.include_router(crimes.router, prefix=settings.api_prefix) # /api/v1/crimes/...
app.include_router(reports.router, prefix=settings.api_prefix) # /api/v1/reports/...
