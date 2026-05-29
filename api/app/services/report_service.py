"""AI crime report generation service.

Queries the database for crime statistics, builds a grounded prompt,
and calls Gemini. Self-contained — no pandas dependency.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.crime import Crime
from app.utils.gemini import call_gemini

logger = logging.getLogger(__name__)


def generate_crime_report(
    db: Session,
    force: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """Generate an AI briefing report for a given force and optional date range.

    Returns:
        Dict with keys: report, force, total_crimes, period_covered, generated_at.

    Raises:
        ValueError: No crime data found.
        EnvironmentError: GEMINI_API_KEY not configured.
        RuntimeError: Gemini API failure.
    """
    stats = _query_statistics(db, force, start_date, end_date)

    if stats["total_crimes"] == 0:
        raise ValueError(
            f"No crime data found for force='{force}'"
            + (f" between {start_date} and {end_date}" if start_date or end_date else "")
            + ". Seed the database first."
        )

    logger.info(
        "Generating report for force='%s', total=%d, period=%s",
        stats["force"], stats["total_crimes"], stats["period_covered"],
    )

    system = _system_prompt()
    user = _user_prompt(stats)
    report_text = call_gemini(system, user)

    return {
        "report": report_text,
        "force": stats["force"],
        "total_crimes": stats["total_crimes"],
        "period_covered": stats["period_covered"],
        "generated_at": datetime.now(timezone.utc),
    }


def _get_filters(force, start_date, end_date):
    filters = [Crime.force.ilike(f"%{force}%")]
    if start_date:
        filters.append(Crime.month >= start_date)
    if end_date:
        filters.append(Crime.month <= end_date)
    return filters


def _query_statistics(db: Session, force: str, start_date, end_date) -> dict:
    filters = _get_filters(force, start_date, end_date)

    total = db.query(Crime).filter(*filters).count()
    if total == 0:
        return {"force": force, "total_crimes": 0, "period_covered": "N/A", "distribution": {}, "top_categories": [], "under_investigation_pct": 0.0}

    # Actual force name from data
    row = db.query(Crime.force).filter(*filters).first()
    force_name = row[0] if row else force

    # Crime type distribution
    type_counts = (
        db.query(Crime.crime_type, func.count(Crime.id).label("cnt"))
        .filter(*filters)
        .group_by(Crime.crime_type)
        .order_by(func.count(Crime.id).desc())
        .all()
    )
    distribution = {r[0]: round(r[1] / total * 100, 1) for r in type_counts}
    top_3 = [r[0] for r in type_counts[:3]]

    # Period covered
    month_rows = (
        db.query(Crime.month)
        .filter(*filters)
        .distinct()
        .order_by(Crime.month)
        .all()
    )
    months = [r[0] for r in month_rows]
    if len(months) > 1:
        period_covered = f"{months[0]} to {months[-1]}"
    elif months:
        period_covered = months[0]
    else:
        period_covered = "N/A"

    # Under investigation
    under_inv = db.query(Crime).filter(*filters).filter(Crime.outcome.ilike("%investigation%")).count()
    under_inv_pct = round(under_inv / total * 100, 1)

    return {
        "force": force_name,
        "total_crimes": total,
        "period_covered": period_covered,
        "distribution": distribution,
        "top_categories": top_3,
        "under_investigation_pct": under_inv_pct,
    }


def _system_prompt() -> str:
    return (
        "You are a professional crime data analyst producing formal briefing "
        "reports for local government and law enforcement audiences.\n\n"
        "STRICT RULES:\n"
        "1. Report ONLY statistics explicitly provided in the input data.\n"
        "2. Do NOT introduce comparisons, national averages, or external context.\n"
        "3. Do NOT speculate about causes, trends, or future outcomes.\n"
        "4. Write EXACTLY three paragraphs with these headings on their own line:\n"
        "   Overview | Crime Breakdown | Implications\n"
        "5. Every statistic you cite must be traceable to the input data.\n"
        "Reports that introduce unverified statistics will be rejected."
    )


def _user_prompt(stats: dict) -> str:
    distribution_lines = "\n".join(
        f"  - {crime}: {pct}%" for crime, pct in stats["distribution"].items()
    )
    return (
        "Generate an analytical crime summary report using ONLY the data below.\n\n"
        f"Force area: {stats['force']}\n"
        f"Reporting period: {stats['period_covered']}\n"
        f"Total crimes recorded: {stats['total_crimes']:,}\n"
        f"Under investigation: {stats['under_investigation_pct']}%\n\n"
        f"Crime type breakdown:\n{distribution_lines}\n\n"
        f"Top three categories: {', '.join(stats['top_categories'])}\n\n"
        "Write a formal three-paragraph report "
        "(Overview | Crime Breakdown | Implications) "
        "suitable for a local government briefing. "
        "Cite only the figures provided above."
    )
