from pydantic import BaseModel, field_validator
from typing import Any


class AskRequest(BaseModel):
    question: str

    @field_validator("question")
    @classmethod
    def question_not_empty(cls, v):
        if not v.strip():
            raise ValueError("Question cannot be empty")
        if len(v) > 500:
            raise ValueError("Question must be 500 characters or fewer")
        return v.strip()


class AskResponse(BaseModel):
    answer: str
    sources: list[dict[str, Any]]
    confidence: float
    records_analysed: int
