"""RAG answer generation — retrieves crime data, builds context, calls Gemini."""
import logging
from typing import Optional

from sqlalchemy.orm import Session

from app.services.rag.retrieval import parse_question, retrieve
from app.services.rag.context_builder import build_context
from app.services.rag.prompts import SYSTEM_PROMPT, build_user_prompt
from app.utils.gemini import call_gemini

logger = logging.getLogger(__name__)


def answer_question(db: Session, question: str) -> dict:
    """Answer a natural language crime question using SQL retrieval + Gemini.

    Pipeline:
      1. Parse question → extract force/crime_type/month filters
      2. Retrieve matching crime statistics from database
      3. Build plain-text context
      4. Generate grounded answer with Gemini

    Returns:
        Dict with keys: answer, sources, confidence, records_analysed.

    Raises:
        EnvironmentError: GEMINI_API_KEY not set.
        RuntimeError: Gemini API failure.
    """
    filters = parse_question(question)
    logger.info("RAG query — filters: %s", filters)

    data = retrieve(db, **filters)
    logger.info("RAG retrieved %d records", data["total"])

    if data["total"] == 0:
        return {
            "answer": (
                "No crime data was found matching your query in the database. "
                "Try asking about West Yorkshire Police or a different time period."
            ),
            "sources": [],
            "confidence": 0.0,
            "records_analysed": 0,
        }

    context = build_context(data)
    user_prompt = build_user_prompt(question, context)

    answer_text = call_gemini(
        system=SYSTEM_PROMPT,
        user=user_prompt,
        max_tokens=500,
        temperature=0.1,
    )

    # Confidence heuristic: more data and specific filters = higher confidence
    filter_count = sum(1 for v in filters.values() if v is not None)
    confidence = min(0.95, 0.6 + (filter_count * 0.1) + (min(data["total"], 1000) / 10000))

    return {
        "answer": answer_text,
        "sources": data["sample_records"],
        "confidence": round(confidence, 2),
        "records_analysed": data["total"],
    }
