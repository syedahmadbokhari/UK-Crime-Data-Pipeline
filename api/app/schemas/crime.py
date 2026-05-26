from pydantic import BaseModel
from typing import Optional


class CrimeResponse(BaseModel):
    id: int
    month: str
    force: str
    crime_type: str
    lsoa_code: Optional[str] = None
    lsoa_name: Optional[str] = None
    outcome: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    model_config = {"from_attributes": True}


class CrimeSummary(BaseModel):
    force: str
    total_crimes: int
    top_crime_type: str
    under_investigation_pct: float


class PaginatedCrimes(BaseModel):
    total: int
    page: int
    page_size: int
    results: list[CrimeResponse]
