from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.jwt import get_current_user
from app.database import get_async_db
from app.middleware.rate_limit import limiter
from app.repositories import crime_repository as repo
from app.schemas.crime import CrimeResponse, CrimeSummary, CursorPaginatedCrimes, PaginatedCrimes
from app.schemas.rag import AskRequest, AskResponse
from app.services.cache.cache_service import get_cached, set_cached
from app.services.cache.keys import summary_key
from app.services.rag.answer_generator import answer_question

router = APIRouter(prefix="/crimes", tags=["crimes"])


@router.get("/", response_model=PaginatedCrimes)
@limiter.limit("100/minute")
async def list_crimes(
    request: Request,
    force: Optional[str] = Query(None),
    month: Optional[str] = Query(None, description="YYYY-MM"),
    crime_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_async_db),
):
    skip = (page - 1) * page_size
    results, total = await repo.get_crimes(db, force, month, crime_type, skip, page_size)
    return PaginatedCrimes(total=total, page=page, page_size=page_size, results=results)


@router.get("/cursor", response_model=CursorPaginatedCrimes, summary="List Crimes (cursor pagination)")
@limiter.limit("100/minute")
async def list_crimes_cursor(
    request: Request,
    cursor: Optional[int] = Query(None, description="Last seen crime ID — omit for first page"),
    limit: int = Query(20, ge=1, le=200),
    force: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    crime_type: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_async_db),
):
    """Cursor-based pagination — more efficient than offset on large datasets.
    Pass next_cursor from the previous response to get the next page.
    """
    crimes, next_cursor = await repo.get_crimes_by_cursor(db, cursor, limit, force, month, crime_type)
    return CursorPaginatedCrimes(data=crimes, next_cursor=next_cursor, has_more=next_cursor is not None)


@router.get("/summary", response_model=CrimeSummary)
@limiter.limit("20/minute")
async def crime_summary(
    request: Request,
    force: str = Query(...),
    db: AsyncSession = Depends(get_async_db),
    _: dict = Depends(get_current_user),
):
    key = summary_key(force)
    cached = await get_cached(key)
    if cached:
        return cached
    result = await repo.get_summary(db, force)
    await set_cached(key, result)
    return result


@router.post("/ask", response_model=AskResponse, summary="Natural Language Crime Query (RAG)")
@limiter.limit("20/minute")
async def ask_crime_question(
    request: Request,
    body: AskRequest,
    db: AsyncSession = Depends(get_async_db),
    _: dict = Depends(get_current_user),
):
    try:
        return await answer_question(db, body.question)
    except EnvironmentError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc))


@router.get("/{crime_id}", response_model=CrimeResponse)
@limiter.limit("100/minute")
async def get_crime(
    request: Request,
    crime_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    crime = await repo.get_crime_by_id(db, crime_id)
    if not crime:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Crime not found")
    return crime
