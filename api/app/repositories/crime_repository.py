"""Crime repository — SQLAlchemy 2.0 async style."""
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.crime import Crime


async def get_crimes(
    db: AsyncSession,
    force: Optional[str] = None,
    month: Optional[str] = None,
    crime_type: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
) -> tuple[list[Crime], int]:
    stmt = select(Crime)
    if force:
        stmt = stmt.where(Crime.force.ilike(f"%{force}%"))
    if month:
        stmt = stmt.where(Crime.month == month)
    if crime_type:
        stmt = stmt.where(Crime.crime_type.ilike(f"%{crime_type}%"))

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = (await db.execute(count_stmt)).scalar_one()

    result = await db.execute(stmt.order_by(Crime.id).offset(skip).limit(limit))
    return list(result.scalars().all()), total


async def get_crimes_by_cursor(
    db: AsyncSession,
    cursor: Optional[int] = None,
    limit: int = 20,
    force: Optional[str] = None,
    month: Optional[str] = None,
    crime_type: Optional[str] = None,
) -> tuple[list[Crime], Optional[int]]:
    """Cursor-based pagination — stable ordering, no drift at scale.

    Fetches limit+1 rows to detect whether more pages exist. Cursor is the
    last-seen crime ID. Much more efficient than OFFSET on large datasets
    because it avoids full table scans.
    """
    stmt = select(Crime).order_by(Crime.id)
    if cursor is not None:
        stmt = stmt.where(Crime.id > cursor)
    if force:
        stmt = stmt.where(Crime.force.ilike(f"%{force}%"))
    if month:
        stmt = stmt.where(Crime.month == month)
    if crime_type:
        stmt = stmt.where(Crime.crime_type.ilike(f"%{crime_type}%"))

    result = await db.execute(stmt.limit(limit + 1))
    crimes = list(result.scalars().all())

    has_more = len(crimes) > limit
    if has_more:
        crimes = crimes[:limit]

    next_cursor = crimes[-1].id if (crimes and has_more) else None
    return crimes, next_cursor


async def get_crime_by_id(db: AsyncSession, crime_id: int) -> Optional[Crime]:
    result = await db.execute(select(Crime).where(Crime.id == crime_id))
    return result.scalar_one_or_none()


async def get_summary(db: AsyncSession, force: str) -> dict:
    base_stmt = select(Crime).where(Crime.force.ilike(f"%{force}%"))
    total = (await db.execute(select(func.count()).select_from(base_stmt.subquery()))).scalar_one()

    if total == 0:
        return {
            "force": force,
            "total_crimes": 0,
            "top_crime_type": "N/A",
            "under_investigation_pct": 0.0,
        }

    top_type_result = await db.execute(
        select(Crime.crime_type, func.count(Crime.id).label("cnt"))
        .where(Crime.force.ilike(f"%{force}%"))
        .group_by(Crime.crime_type)
        .order_by(func.count(Crime.id).desc())
        .limit(1)
    )
    top_type_row = top_type_result.first()

    under_inv = (
        await db.execute(
            select(func.count())
            .select_from(Crime)
            .where(Crime.force.ilike(f"%{force}%"))
            .where(Crime.outcome.ilike("%investigation%"))
        )
    ).scalar_one()

    return {
        "force": force,
        "total_crimes": total,
        "top_crime_type": top_type_row[0] if top_type_row else "N/A",
        "under_investigation_pct": round((under_inv / total) * 100, 1),
    }
