from sqlalchemy.orm import Session
from sqlalchemy import func
from app.models.crime import Crime
from typing import Optional


def get_crimes(
    db: Session,
    force: Optional[str] = None,
    month: Optional[str] = None,
    crime_type: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
) -> tuple[list[Crime], int]:
    query = db.query(Crime)
    if force:
        query = query.filter(Crime.force.ilike(f"%{force}%"))
    if month:
        query = query.filter(Crime.month == month)
    if crime_type:
        query = query.filter(Crime.crime_type.ilike(f"%{crime_type}%"))
    total = query.count()
    results = query.offset(skip).limit(limit).all()
    return results, total


def get_crime_by_id(db: Session, crime_id: int) -> Optional[Crime]:
    return db.query(Crime).filter(Crime.id == crime_id).first()


def get_summary(db: Session, force: str) -> dict:
    base = db.query(Crime).filter(Crime.force.ilike(f"%{force}%"))
    total = base.count()
    if total == 0:
        return {"force": force, "total_crimes": 0, "top_crime_type": "N/A", "under_investigation_pct": 0.0}

    top_type = (
        db.query(Crime.crime_type, func.count(Crime.id).label("cnt"))
        .filter(Crime.force.ilike(f"%{force}%"))
        .group_by(Crime.crime_type)
        .order_by(func.count(Crime.id).desc())
        .first()
    )
    under_inv = base.filter(Crime.outcome.ilike("%investigation%")).count()
    return {
        "force": force,
        "total_crimes": total,
        "top_crime_type": top_type[0] if top_type else "N/A",
        "under_investigation_pct": round((under_inv / total) * 100, 1),
    }
