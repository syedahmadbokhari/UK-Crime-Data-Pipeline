"""Versioned prompt builder for the AI Crime Report Generator.

v1 — simple summarisation prompt, minimal constraints.
v2 — strict anti-hallucination prompt with explicit output structure rules.
"""

import logging

logger = logging.getLogger(__name__)

SUPPORTED_VERSIONS: tuple[str, ...] = ("v1", "v2")


class PromptBuilder:
    """Constructs versioned (system, user) prompt pairs from crime feature dicts.

    Usage:
        builder = PromptBuilder()
        system, user = builder.build(features, version="v2")
    """

    def build(self, features: dict, version: str = "v2") -> tuple[str, str]:
        """Return a (system_prompt, user_prompt) pair for the given features.

        Args:
            features: Output dict from features.extract_features().
            version: Prompt template version. Default is 'v2' (recommended).

        Returns:
            Tuple of (system message string, user message string).

        Raises:
            ValueError: If version is not one of SUPPORTED_VERSIONS.
        """
        if version not in SUPPORTED_VERSIONS:
            raise ValueError(
                f"Unsupported prompt version '{version}'. "
                f"Choose from {SUPPORTED_VERSIONS}."
            )

        system = self._system_v1() if version == "v1" else self._system_v2()
        user = self._user_prompt(features)

        logger.debug(
            "Built prompt version=%s for force='%s', period='%s'",
            version, features.get("force"), features.get("period"),
        )
        return system, user

    # ── System prompts ────────────────────────────────────────────────────────

    @staticmethod
    def _system_v1() -> str:
        return (
            "You are a professional crime data analyst writing briefing reports "
            "for local government audiences. Write in clear, formal English. "
            "Structure your response as three paragraphs: "
            "Overview, Crime Breakdown, and Implications."
        )

    @staticmethod
    def _system_v2() -> str:
        return (
            "You are a professional crime data analyst producing formal briefing "
            "reports for local government and law enforcement audiences.\n\n"
            "STRICT RULES — you must follow every one:\n"
            "1. Report ONLY statistics and facts explicitly provided in the input data.\n"
            "2. Do NOT introduce comparisons, national averages, or external context "
            "   unless it appears in the input.\n"
            "3. Do NOT speculate about causes, trends, or future outcomes.\n"
            "4. Do NOT use emotive, sensationalist, or informal language.\n"
            "5. Write EXACTLY three paragraphs with these headings on their own line:\n"
            "   Overview | Crime Breakdown | Implications\n"
            "6. Every statistic you cite must be directly traceable to the input data.\n"
            "7. If month-on-month data is provided, include it in the Overview paragraph.\n\n"
            "Reports that introduce unverified statistics will be rejected."
        )

    # ── User prompt ───────────────────────────────────────────────────────────

    @staticmethod
    def _user_prompt(features: dict) -> str:
        distribution_lines = "\n".join(
            f"  - {crime}: {pct}%"
            for crime, pct in features["distribution"].items()
        )

        mom_section = ""
        mom = features.get("mom_change")
        if mom and mom.get("absolute") is not None:
            sign = "+" if mom["absolute"] > 0 else ""
            mom_section = (
                f"\nMonth-on-month change: {mom['direction']} of {mom['pct']}% "
                f"({sign}{mom['absolute']:,} crimes vs previous period "
                f"[{mom['previous_total']:,} crimes])"
            )

        return (
            "Generate an analytical crime summary report using ONLY the data below.\n\n"
            f"Force area: {features['force']}\n"
            f"Reporting period: {features['period']}\n"
            f"Total crimes recorded: {features['total_crimes']:,}"
            f"{mom_section}\n\n"
            f"Crime type breakdown:\n{distribution_lines}\n\n"
            f"Top three categories: {', '.join(features['top_categories'])}\n\n"
            "Write a formal three-paragraph report "
            "(Overview | Crime Breakdown | Implications) "
            "suitable for a local government briefing. "
            "Cite only the figures provided above."
        )
