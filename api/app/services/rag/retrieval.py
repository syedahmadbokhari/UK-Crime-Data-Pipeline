"""SQL retrieval layer — async, fetches crime statistics as RAG context."""
import logging
import re
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.crime import Crime

logger = logging.getLogger(__name__)

_FORCE_KEYWORDS: dict[str, str] = {
    "west yorkshire": "West Yorkshire",
    "yorkshire": "West Yorkshire",
    "bradford": "West Yorkshire",
    "leeds": "West Yorkshire",
    "wakefield": "West Yorkshire",
    "huddersfield": "West Yorkshire",
    "halifax": "West Yorkshire",
}
_CRIME_KEYWORDS = [
    "burglary", "theft", "violence", "robbery", "fraud", "drugs",
    "shoplifting", "vandalism", "arson", "criminal damage",
    "anti-social", "antisocial", "public order", "weapon",
]


def parse_question(question: str) -> dict:
    q = question.lower()
    filters: dict[str, Optional[str]] = {"force": None, "crime_type": None, "month": None}
    for keyword, force in _FORCE_KEYWORDS.items():
        if keyword in q:
            filters["force"] = force
            break
    for crime in _CRIME_KEYWORDS:
        if crime in q:
            filters["crime_type"] = crime
            break
    match = re.search(r"\b(\d{4})-(\d{2})\b", question)
    if match:
        filters["month"] = match.group(0)
    logger.debug("Parsed question filters: %s", filters)
    return filters


async def retrieve(
    db: AsyncSession,
    force: Optional[str] = None,
    crime_type: Optional[str] = None,
    month: Optional[str] = None,
) -> dict:
    filters = []
    if force:
        filters.append(Crime.force.ilike(f"%{force}%"))
    if crime_type:
        filters.append(Crime.crime_type.ilike(f"%{crime_type}%"))
    if month:
        filters.append(Crime.month == month)

    base_stmt = select(Crime)
    if filters:
        base_stmt = base_stmt.where(*filters)

    total = (await db.execute(select(func.count()).select_from(base_stmt.subquery()))).scalar_one()
    if total == 0:
        return {"total": 0, "force": force or "all forces", "months": [], "type_distribution": [], "outcome_distribution": [], "sample_records": []}

    force_stmt = select(Crime.force)
    if filters:
        force_stmt = force_stmt.where(*filters)
    force_row = (await db.execute(force_stmt.limit(1))).first()
    force_name = force_row[0] if force_row else (force or "Multiple forces")

    type_dist = (await db.execute(
        select(Crime.crime_type, func.count(Crime.id).label("cnt"))
        .where(*filters).group_by(Crime.crime_type).order_by(func.count(Crime.id).desc())
    )).all() if filters else (await db.execute(
        select(Crime.crime_type, func.count(Crime.id).label("cnt"))
        .group_by(Crime.crime_type).order_by(func.count(Crime.id).desc())
    )).all()

    outcome_stmt = select(Crime.outcome, func.count(Crime.id).label("cnt")).group_by(Crime.outcome).order_by(func.count(Crime.id).desc()).limit(5)
    if filters:
        outcome_stmt = outcome_stmt.where(*filters)
    outcome_dist = (await db.execute(outcome_stmt)).all()

    months_stmt = select(Crime.month).distinct().order_by(Crime.month)
    if filters:
        months_stmt = months_stmt.where(*filters)
    months = list((await db.execute(months_stmt)).scalars().all())

    sample_result = await db.execute(base_stmt.limit(5))
    sample = list(sample_result.scalars().all())

    return {
        "total": total,
        "force": force_name,
        "months": months,
        "type_distribution": [{"crime_type": r[0], "count": r[1], "pct": round(r[1] / total * 100, 1)} for r in type_dist],
        "outcome_distribution": [{"outcome": r[0] or "Unknown", "count": r[1]} for r in outcome_dist],
        "sample_records": [{"id": r.id, "month": r.month, "force": r.force, "crime_type": r.crime_type, "lsoa_name": r.lsoa_name} for r in sample],
    }
