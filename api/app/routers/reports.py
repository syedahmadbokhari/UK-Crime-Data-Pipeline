"""AI crime report generation endpoint."""
import logging

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.auth.jwt import get_current_user
from app.database import get_db
from app.middleware.rate_limit import limiter
from app.schemas.report import ReportRequest, ReportResponse
from app.services.report_service import generate_crime_report

router = APIRouter(prefix="/reports", tags=["reports"])
logger = logging.getLogger(__name__)


@router.post(
    "/generate",
    response_model=ReportResponse,
    summary="Generate AI Crime Briefing Report",
    description=(
        "Generate a formal AI crime briefing report for a given police force "
        "and optional date range. Uses Gemini to produce a three-paragraph "
        "analytical narrative grounded entirely in database crime statistics. "
        "Requires JWT authentication."
    ),
)
@limiter.limit("10/minute")
def generate_report(
    request: Request,
    body: ReportRequest,
    db: Session = Depends(get_db),
    _: dict = Depends(get_current_user),
):
    logger.info("Report request: force='%s' %s–%s", body.force, body.start_date, body.end_date)
    try:
        result = generate_crime_report(
            db=db,
            force=body.force,
            start_date=body.start_date,
            end_date=body.end_date,
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except EnvironmentError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))
    except RuntimeError as exc:
        logger.error("Report generation failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc))
