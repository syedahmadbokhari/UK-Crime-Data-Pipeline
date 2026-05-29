"""SQL retrieval layer — fetches crime statistics as structured RAG context."""
import logging
import re
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.crime import Crime

logger = logging.getLogger(__name__)

# Simple keyword map: phrase in question → force partial match
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
    "burglary", "theft", "violence", "robbery", "fraud",
    "drugs", "shoplifting", "vandalism", "arson", "criminal damage",
    "anti-social", "antisocial", "public order", "weapon",
]


def parse_question(question: str) -> dict:
    """Extract structured filters from a natural language question."""
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

    month_match = re.search(r"\b(\d{4})-(\d{2})\b", question)
    if month_match:
        filters["month"] = month_match.group(0)

    logger.debug("Parsed question filters: %s", filters)
    return filters


def retrieve(
    db: Session,
    force: Optional[str] = None,
    crime_type: Optional[str] = None,
    month: Optional[str] = None,
) -> dict:
    """Query the database and return structured context for the RAG pipeline."""
    filters = []
    if force:
        filters.append(Crime.force.ilike(f"%{force}%"))
    if crime_type:
        filters.append(Crime.crime_type.ilike(f"%{crime_type}%"))
    if month:
        filters.append(Crime.month == month)

    base = db.query(Crime)
    if filters:
        base = base.filter(*filters)

    total = base.count()
    if total == 0:
        return {
            "total": 0,
            "force": force or "all forces",
            "months": [],
            "type_distribution": [],
            "outcome_distribution": [],
            "sample_records": [],
        }

    # Force name from data
    force_row = db.query(Crime.force).filter(*filters).first() if filters else db.query(Crime.force).first()
    force_name = force_row[0] if force_row else (force or "Multiple forces")

    # Crime type distribution
    type_dist = (
        db.query(Crime.crime_type, func.count(Crime.id).label("cnt"))
        .filter(*filters)
        .group_by(Crime.crime_type)
        .order_by(func.count(Crime.id).desc())
        .all()
    )

    # Outcome distribution
    outcome_dist = (
        db.query(Crime.outcome, func.count(Crime.id).label("cnt"))
        .filter(*filters)
        .group_by(Crime.outcome)
        .order_by(func.count(Crime.id).desc())
        .limit(5)
        .all()
    )

    # Distinct months
    month_rows = (
        db.query(Crime.month)
        .filter(*filters)
        .distinct()
        .order_by(Crime.month)
        .all()
    )
    months = [r[0] for r in month_rows]

    # Sample records for source attribution
    sample = base.limit(5).all()

    return {
        "total": total,
        "force": force_name,
        "months": months,
        "type_distribution": [
            {"crime_type": r[0], "count": r[1], "pct": round(r[1] / total * 100, 1)}
            for r in type_dist
        ],
        "outcome_distribution": [
            {"outcome": r[0] or "Unknown", "count": r[1]}
            for r in outcome_dist
        ],
        "sample_records": [
            {
                "id": r.id,
                "month": r.month,
                "force": r.force,
                "crime_type": r.crime_type,
                "lsoa_name": r.lsoa_name,
            }
            for r in sample
        ],
    }
