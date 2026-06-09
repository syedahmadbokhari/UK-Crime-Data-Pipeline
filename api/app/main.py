from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from app.database import Base, engine
from app.middleware.logging import LoggingMiddleware
from app.middleware.rate_limit import limiter
from app.routers import auth, crimes, health, reports


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Tables are managed by Alembic migrations (alembic upgrade head).
    # create_all is kept as a fallback for local dev without a migration run.
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="Crime Data API",
    description=(
        "REST API for UK Police crime data. "
        "Provides filtering, pagination, force-level summaries, "
        "AI-generated briefing reports, and natural language crime queries. "
        "Protected endpoints require JWT authentication."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

# Middleware (added in reverse order — last added runs first)
app.add_middleware(LoggingMiddleware)
app.add_middleware(SlowAPIMiddleware)

# Rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.include_router(health.router)
app.include_router(auth.router)
app.include_router(crimes.router)
app.include_router(reports.router)
