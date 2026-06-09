"""In-memory job store for background report generation.

For production at scale, replace _jobs with a Redis hash or a jobs table.
Current limitation: jobs are lost on server restart.
"""
import uuid
from datetime import datetime, timezone
from typing import Any, Literal, Optional

JobStatus = Literal["queued", "processing", "completed", "failed"]

_jobs: dict[str, dict] = {}


def create_job() -> str:
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "result": None,
        "error": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
    }
    return job_id


def get_job(job_id: str) -> Optional[dict]:
    return _jobs.get(job_id)


def update_job(
    job_id: str,
    status: JobStatus,
    result: Any = None,
    error: Optional[str] = None,
) -> None:
    if job_id not in _jobs:
        return
    _jobs[job_id].update({
        "status": status,
        "result": result,
        "error": error,
        "completed_at": datetime.now(timezone.utc).isoformat() if status in ("completed", "failed") else None,
    })
