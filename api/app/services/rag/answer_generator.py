"""RAG answer generation — async, retrieves DB context then calls Gemini."""
import logging
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.rag.retrieval import parse_question, retrieve
from app.services.rag.context_builder import build_context
from app.services.rag.prompts import SYSTEM_PROMPT, build_user_prompt
from app.utils.gemini import call_gemini

logger = logging.getLogger(__name__)


async def answer_question(db: AsyncSession, question: str) -> dict:
    """Answer a natural language crime question via SQL retrieval + Gemini."""
    filters = parse_question(question)
    logger.info("RAG filters: %s", filters)

    data = await retrieve(db, **filters)
    logger.info("RAG retrieved %d records", data["total"])

    if data["total"] == 0:
        return {
            "answer": "No crime data found matching your query. Try asking about West Yorkshire Police or a specific time period.",
            "sources": [],
            "confidence": 0.0,
            "records_analysed": 0,
        }

    context = build_context(data)
    answer_text = call_gemini(SYSTEM_PROMPT, build_user_prompt(question, context), max_tokens=500, temperature=0.1)

    filter_count = sum(1 for v in filters.values() if v is not None)
    confidence = min(0.95, 0.6 + (filter_count * 0.1) + (min(data["total"], 1000) / 10000))

    return {
        "answer": answer_text,
        "sources": data["sample_records"],
        "confidence": round(confidence, 2),
        "records_analysed": data["total"],
    }
