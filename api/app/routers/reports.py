"""AI crime report generation with background task processing.

POST /reports/generate  → queues job, returns immediately with job_id
GET  /reports/{job_id}  → returns job status + report when complete

Design: FastAPI BackgroundTasks (in-process, simple).
Production upgrade path: replace _jobs dict with Redis + Celery workers.
"""
import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.jwt import get_current_user
from app.database import AsyncSessionLocal, get_async_db
from app.middleware.rate_limit import limiter
from app.schemas.report import ReportRequest
from app.services.jobs import create_job, get_job, update_job
from app.services.report_service import generate_crime_report

router = APIRouter(prefix="/reports", tags=["reports"])
logger = logging.getLogger(__name__)


async def _run_report(job_id: str, force: str, start_date, end_date) -> None:
    """Background task — creates its own DB session (request session is closed)."""
    update_job(job_id, "processing")
    try:
        async with AsyncSessionLocal() as db:
            result = await generate_crime_report(db, force, start_date, end_date)
        result["generated_at"] = result["generated_at"].isoformat()
        update_job(job_id, "completed", result=result)
        logger.info("Job %s completed", job_id)
    except Exception as exc:
        update_job(job_id, "failed", error=str(exc))
        logger.error("Job %s failed: %s", job_id, exc)


@router.post(
    "/generate",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Queue AI Crime Report Generation",
    description=(
        "Queues an AI crime report generation job and returns immediately. "
        "Poll GET /reports/{job_id} for status and result. "
        "Requires JWT authentication."
    ),
)
@limiter.limit("10/minute")
async def generate_report(
    request: Request,
    body: ReportRequest,
    background_tasks: BackgroundTasks,
    _: dict = Depends(get_current_user),
):
    job_id = create_job()
    background_tasks.add_task(_run_report, job_id, body.force, body.start_date, body.end_date)
    logger.info("Report job %s queued for force='%s'", job_id, body.force)
    return {"job_id": job_id, "status": "queued"}


@router.get("/{job_id}", summary="Get Report Job Status")
@limiter.limit("60/minute")
async def get_report_status(
    request: Request,
    job_id: str,
    _: dict = Depends(get_current_user),
):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job
