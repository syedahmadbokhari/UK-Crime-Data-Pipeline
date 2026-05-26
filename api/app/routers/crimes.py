from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from app.database import get_db
from app.schemas.crime import CrimeResponse, CrimeSummary, PaginatedCrimes
from app.repositories.crime_repository import get_crimes, get_crime_by_id, get_summary
from app.auth.jwt import get_current_user

router = APIRouter(prefix="/crimes", tags=["crimes"])


@router.get("/", response_model=PaginatedCrimes)
def list_crimes(
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
def crime_summary(
    force: str = Query(..., description="Police force name"),
    db: Session = Depends(get_db),
    _: dict = Depends(get_current_user),
):
    return get_summary(db, force)


@router.get("/{crime_id}", response_model=CrimeResponse)
def get_crime(crime_id: int, db: Session = Depends(get_db)):
    crime = get_crime_by_id(db, crime_id)
    if not crime:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Crime not found")
    return crime
