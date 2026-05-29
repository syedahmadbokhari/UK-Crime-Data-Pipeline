from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from app.auth.jwt import get_current_user
from app.database import get_db
from app.middleware.rate_limit import limiter
from app.repositories.crime_repository import get_crimes, get_crime_by_id, get_summary
from app.schemas.crime import CrimeResponse, CrimeSummary, PaginatedCrimes
from app.schemas.rag import AskRequest, AskResponse
from app.services.cache.cache_service import get_cached, set_cached
from app.services.cache.keys import summary_key
from app.services.rag.answer_generator import answer_question

router = APIRouter(prefix="/crimes", tags=["crimes"])


@router.get("/", response_model=PaginatedCrimes)
@limiter.limit("100/minute")
def list_crimes(
    request: Request,
    force: str = Query(None, description="Filter by police force name"),
    month: str = Query(None, description="Filter by month (YYYY-MM)"),
    crime_type: str = Query(None, description="Filter by crime type"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=500, description="Results per page"),
    db: Session = Depends(get_db),
):
    skip = (page - 1) * page_size
    results, total = get_crimes(db, force, month, crime_type, skip, page_size)
    return PaginatedCrimes(total=total, page=page, page_size=page_size, results=results)


@router.get("/summary", response_model=CrimeSummary)
@limiter.limit("20/minute")
def crime_summary(
    request: Request,
    force: str = Query(..., description="Police force name"),
    db: Session = Depends(get_db),
    _: dict = Depends(get_current_user),
):
    key = summary_key(force)
    cached = get_cached(key)
    if cached:
        return cached

    result = get_summary(db, force)
    set_cached(key, result)
    return result


@router.post(
    "/ask",
    response_model=AskResponse,
    summary="Ask a natural language question about crime data",
    description=(
        "Submit a natural language question about UK crime data. "
        "The system retrieves relevant statistics from the database and "
        "uses Gemini to generate a grounded answer. "
        "Requires JWT authentication."
    ),
)
@limiter.limit("20/minute")
def ask_crime_question(
    request: Request,
    body: AskRequest,
    db: Session = Depends(get_db),
    _: dict = Depends(get_current_user),
):
    try:
        return answer_question(db, body.question)
    except EnvironmentError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc))


@router.get("/{crime_id}", response_model=CrimeResponse)
@limiter.limit("100/minute")
def get_crime(
    request: Request,
    crime_id: int,
    db: Session = Depends(get_db),
):
    crime = get_crime_by_id(db, crime_id)
    if not crime:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Crime not found")
    return crime
