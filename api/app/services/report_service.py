"""AI crime report generation — async, self-contained, no pandas dependency."""
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.crime import Crime
from app.utils.gemini import call_gemini

logger = logging.getLogger(__name__)


async def generate_crime_report(
    db: AsyncSession,
    force: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """Generate an AI briefing report for a given force and optional date range.

    Raises:
        ValueError: No crime data found.
        EnvironmentError: GEMINI_API_KEY not configured.
        RuntimeError: Gemini API failure.
    """
    stats = await _query_statistics(db, force, start_date, end_date)

    if stats["total_crimes"] == 0:
        raise ValueError(
            f"No crime data found for force='{force}'"
            + (f" between {start_date} and {end_date}" if start_date or end_date else "")
            + ". Seed the database first."
        )

    logger.info("Generating report: force='%s' total=%d period=%s", stats["force"], stats["total_crimes"], stats["period_covered"])
    report_text = call_gemini(_system_prompt(), _user_prompt(stats))

    return {
        "report": report_text,
        "force": stats["force"],
        "total_crimes": stats["total_crimes"],
        "period_covered": stats["period_covered"],
        "generated_at": datetime.now(timezone.utc),
    }


def _get_filters(force: str, start_date: Optional[str], end_date: Optional[str]) -> list:
    filters = [Crime.force.ilike(f"%{force}%")]
    if start_date:
        filters.append(Crime.month >= start_date)
    if end_date:
        filters.append(Crime.month <= end_date)
    return filters


async def _query_statistics(db: AsyncSession, force: str, start_date: Optional[str], end_date: Optional[str]) -> dict:
    filters = _get_filters(force, start_date, end_date)

    total = (await db.execute(select(func.count()).select_from(Crime).where(*filters))).scalar_one()
    if total == 0:
        return {"force": force, "total_crimes": 0, "period_covered": "N/A", "distribution": {}, "top_categories": [], "under_investigation_pct": 0.0}

    force_row = (await db.execute(select(Crime.force).where(*filters).limit(1))).first()
    force_name = force_row[0] if force_row else force

    type_rows = (await db.execute(
        select(Crime.crime_type, func.count(Crime.id).label("cnt"))
        .where(*filters).group_by(Crime.crime_type).order_by(func.count(Crime.id).desc())
    )).all()
    distribution = {r[0]: round(r[1] / total * 100, 1) for r in type_rows}
    top_3 = [r[0] for r in type_rows[:3]]

    month_rows = (await db.execute(
        select(Crime.month).where(*filters).distinct().order_by(Crime.month)
    )).scalars().all()
    months = list(month_rows)
    period_covered = f"{months[0]} to {months[-1]}" if len(months) > 1 else (months[0] if months else "N/A")

    under_inv = (await db.execute(
        select(func.count()).select_from(Crime).where(*filters).where(Crime.outcome.ilike("%investigation%"))
    )).scalar_one()

    return {
        "force": force_name,
        "total_crimes": total,
        "period_covered": period_covered,
        "distribution": distribution,
        "top_categories": top_3,
        "under_investigation_pct": round(under_inv / total * 100, 1),
    }


def _system_prompt() -> str:
    return (
        "You are a professional crime data analyst producing formal briefing reports.\n\n"
        "STRICT RULES:\n"
        "1. Report ONLY statistics explicitly provided in the input data.\n"
        "2. Do NOT introduce external context, national averages, or speculation.\n"
        "3. Write EXACTLY three paragraphs: Overview | Crime Breakdown | Implications\n"
        "4. Every statistic must be traceable to the input data."
    )


def _user_prompt(stats: dict) -> str:
    distribution_lines = "\n".join(f"  - {k}: {v}%" for k, v in stats["distribution"].items())
    return (
        f"Force area: {stats['force']}\nPeriod: {stats['period_covered']}\n"
        f"Total crimes: {stats['total_crimes']:,}\nUnder investigation: {stats['under_investigation_pct']}%\n\n"
        f"Breakdown:\n{distribution_lines}\n\nTop 3: {', '.join(stats['top_categories'])}\n\n"
        "Write a formal three-paragraph report (Overview | Crime Breakdown | Implications)."
    )
