"""Shared Gemini API client used by report and RAG services."""
import logging
import os
import time

from app.config import settings

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gemini-2.5-flash"
_MAX_RETRIES = 3
_BASE_DELAY = 2.0


def call_gemini(
    system: str,
    user: str,
    model: str = DEFAULT_MODEL,
    max_tokens: int = 2000,
    temperature: float = 0.3,
) -> str:
    """Send a prompt to Gemini and return the text response.

    Raises:
        EnvironmentError: GEMINI_API_KEY not set.
        RuntimeError: All retries exhausted or non-retryable error.
    """
    api_key = settings.gemini_api_key or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "GEMINI_API_KEY is not configured. "
            "Add it to your environment variables."
        )

    from google import genai
    from google.genai import types

    client = genai.Client(api_key=api_key)

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            logger.info("Gemini call — attempt %d/%d model=%s", attempt, _MAX_RETRIES, model)
            response = client.models.generate_content(
                model=model,
                contents=user,
                config=types.GenerateContentConfig(
                    system_instruction=system,
                    max_output_tokens=max_tokens,
                    temperature=temperature,
                ),
            )
            logger.info("Gemini response — %d chars", len(response.text))
            return response.text

        except Exception as exc:
            err = str(exc).lower()
            retryable = any(k in err for k in ("quota", "rate", "429", "503", "unavailable", "overloaded"))
            if retryable and attempt < _MAX_RETRIES:
                delay = _BASE_DELAY * (2 ** (attempt - 1))
                logger.warning("Retrying in %.1fs — %s", delay, exc)
                time.sleep(delay)
            else:
                logger.error("Gemini error: %s", exc)
                raise RuntimeError(f"Gemini API error: {exc}") from exc

    raise RuntimeError(f"Gemini failed after {_MAX_RETRIES} attempts.")
