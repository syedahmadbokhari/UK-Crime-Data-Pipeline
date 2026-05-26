from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.database import Base, engine
from app.routers import health, auth, crimes


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="Crime Data API",
    description=(
        "REST API for UK Police crime data. "
        "Provides filtering, pagination, and force-level summaries. "
        "Protected endpoints require JWT authentication."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(auth.router)
app.include_router(crimes.router)
