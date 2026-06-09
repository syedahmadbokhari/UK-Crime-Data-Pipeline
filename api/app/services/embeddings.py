"""Vector embedding service for semantic RAG search.

Requires: pgvector PostgreSQL extension + google-genai for embedding generation.

Architecture:
  User question → text-embedding-004 → 768-dim vector
  → cosine similarity search against crimes.embedding
  → top-k records returned as RAG context

Usage:
    # Generate and store embeddings (run once after seeding)
    await embed_all_crimes(db)

    # Semantic search
    results = await semantic_search(db, question="burglary in Bradford", top_k=20)

Note: embedding column must be populated before semantic_search returns results.
Crimes without embeddings are excluded from vector search.
"""
import logging
import os
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings

logger = logging.getLogger(__name__)

_EMBEDDING_MODEL = "text-embedding-004"
_EMBEDDING_DIM = 768


def _get_api_key() -> str:
    key = settings.gemini_api_key or os.getenv("GEMINI_API_KEY")
    if not key:
        raise EnvironmentError("GEMINI_API_KEY is required for embedding generation.")
    return key


async def generate_embedding(text_input: str) -> list[float]:
    """Generate a 768-dim embedding for the given text using Gemini."""
    from google import genai

    client = genai.Client(api_key=_get_api_key())
    result = client.models.embed_content(
        model=_EMBEDDING_MODEL,
        contents=text_input,
    )
    return result.embeddings[0].values


async def semantic_search(
    db: AsyncSession,
    question: str,
    top_k: int = 20,
) -> list[dict]:
    """Find crimes most semantically similar to the question.

    Returns top_k crime records ordered by cosine similarity.
    Falls back to empty list if pgvector extension is unavailable.
    """
    try:
        query_embedding = await generate_embedding(question)
        embedding_str = f"[{','.join(str(v) for v in query_embedding)}]"

        result = await db.execute(
            text("""
                SELECT id, month, force, crime_type, lsoa_name, outcome,
                       1 - (embedding <=> :embedding::vector) AS similarity
                FROM crimes
                WHERE embedding IS NOT NULL
                ORDER BY embedding <=> :embedding::vector
                LIMIT :top_k
            """),
            {"embedding": embedding_str, "top_k": top_k},
        )
        rows = result.mappings().all()
        return [dict(row) for row in rows]

    except Exception as exc:
        logger.warning("Vector search unavailable (pgvector not installed?): %s", exc)
        return []


async def embed_crime_text(month: str, force: str, crime_type: str, lsoa_name: Optional[str]) -> list[float]:
    """Build a text representation of a crime record and embed it."""
    parts = [crime_type, force, month]
    if lsoa_name:
        parts.append(lsoa_name)
    return await generate_embedding(" | ".join(parts))


async def embed_all_crimes(db: AsyncSession, batch_size: int = 100) -> int:
    """Generate and store embeddings for all crimes that don't have one yet.

    Run this once after seeding the database. Takes ~1-2 minutes for 21k records.
    Returns the number of crimes embedded.
    """
    result = await db.execute(
        text("SELECT id, month, force, crime_type, lsoa_name FROM crimes WHERE embedding IS NULL LIMIT :limit"),
        {"limit": batch_size},
    )
    rows = result.mappings().all()

    embedded = 0
    for row in rows:
        try:
            embedding = await embed_crime_text(row["month"], row["force"], row["crime_type"], row.get("lsoa_name"))
            embedding_str = f"[{','.join(str(v) for v in embedding)}]"
            await db.execute(
                text("UPDATE crimes SET embedding = :emb::vector WHERE id = :id"),
                {"emb": embedding_str, "id": row["id"]},
            )
            embedded += 1
        except Exception as exc:
            logger.warning("Failed to embed crime %s: %s", row["id"], exc)

    await db.commit()
    logger.info("Embedded %d crimes", embedded)
    return embedded
