from datetime import datetime
from typing import Optional
from pydantic import BaseModel, field_validator


class ReportRequest(BaseModel):
    force: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def validate_month_format(cls, v):
        if v is None:
            return v
        import re
        if not re.match(r"^\d{4}-\d{2}$", v):
            raise ValueError("Date must be in YYYY-MM format (e.g. 2026-02)")
        return v


class ReportResponse(BaseModel):
    report: str
    force: str
    total_crimes: int
    period_covered: str
    generated_at: datetime
