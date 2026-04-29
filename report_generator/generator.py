"""Google Gemini API client with error handling and retry logic."""

import logging
import os
import time

from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gemini-2.5-flash"
DEFAULT_TEMPERATURE = 0.3
MAX_RETRIES = 3
_BASE_DELAY = 2.0


def generate_report(
    system: str,
    user: str,
    model: str = DEFAULT_MODEL,
    max_tokens: int = 600,
    temperature: float = DEFAULT_TEMPERATURE,
) -> str:
    """Send a prompt to the Gemini API and return the generated report text.

    Args:
        system: System prompt string (role + constraints).
        user: User prompt string (crime statistics and output instructions).
        model: Gemini model identifier.
        max_tokens: Maximum tokens in the response.
        temperature: Sampling temperature (0.0 = deterministic).

    Returns:
        Generated report as a plain string.

    Raises:
        EnvironmentError: If GEMINI_API_KEY is not set.
        RuntimeError: If all retry attempts are exhausted.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "GEMINI_API_KEY is not set. "
            "Add it to your .env file: GEMINI_API_KEY=your-key-here"
        )

    client = genai.Client(api_key=api_key)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                "Gemini API call — attempt %d/%d, model=%s",
                attempt, MAX_RETRIES, model,
            )
            response = client.models.generate_content(
                model=model,
                contents=user,
                config=types.GenerateContentConfig(
                    system_instruction=system,
                    max_output_tokens=max_tokens,
                    temperature=temperature,
                ),
            )
            report = response.text
            logger.info("Report received — %d chars", len(report))
            return report

        except Exception as exc:
            error_str = str(exc).lower()
            retryable = any(k in error_str for k in ("quota", "rate", "429", "503", "unavailable", "overloaded"))
            if retryable:
                delay = _BASE_DELAY * (2 ** (attempt - 1))
                logger.warning("Transient error (attempt %d). Retrying in %.1fs — %s", attempt, delay, exc)
                time.sleep(delay)
            else:
                logger.error("Gemini API error: %s", exc)
                raise RuntimeError(f"Gemini API error: {exc}") from exc

    raise RuntimeError(
        f"Failed to generate report after {MAX_RETRIES} attempts."
    )
