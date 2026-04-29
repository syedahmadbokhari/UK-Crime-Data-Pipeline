"""Tests for src/prompt_builder.py."""

import pytest

from report_generator.prompt_builder import PromptBuilder, SUPPORTED_VERSIONS

SAMPLE_FEATURES = {
    "force": "West Yorkshire Police",
    "period": "2026-02",
    "total_crimes": 21847,
    "top_categories": ["Violence and sexual offences", "Anti-social behaviour", "Burglary"],
    "distribution": {
        "Violence and sexual offences": 41.2,
        "Anti-social behaviour": 18.7,
        "Burglary": 7.1,
    },
}


class TestPromptBuilder:
    def setup_method(self):
        self.builder = PromptBuilder()

    def test_build_returns_two_strings(self):
        system, user = self.builder.build(SAMPLE_FEATURES)
        assert isinstance(system, str)
        assert isinstance(user, str)

    def test_v1_and_v2_produce_different_system_prompts(self):
        sys_v1, _ = self.builder.build(SAMPLE_FEATURES, version="v1")
        sys_v2, _ = self.builder.build(SAMPLE_FEATURES, version="v2")
        assert sys_v1 != sys_v2

    def test_v2_contains_anti_hallucination_rules(self):
        system, _ = self.builder.build(SAMPLE_FEATURES, version="v2")
        assert "STRICT RULES" in system
        assert "ONLY" in system

    def test_user_prompt_contains_force_and_period(self):
        _, user = self.builder.build(SAMPLE_FEATURES)
        assert "West Yorkshire Police" in user
        assert "2026-02" in user

    def test_user_prompt_contains_total_crimes(self):
        _, user = self.builder.build(SAMPLE_FEATURES)
        assert "21,847" in user

    def test_unsupported_version_raises(self):
        with pytest.raises(ValueError, match="Unsupported prompt version"):
            self.builder.build(SAMPLE_FEATURES, version="v99")

    def test_mom_change_included_in_user_prompt(self):
        features = {**SAMPLE_FEATURES, "mom_change": {
            "previous_total": 20000,
            "absolute": 1847,
            "pct": 9.2,
            "direction": "increase",
        }}
        _, user = self.builder.build(features)
        assert "increase" in user.lower()
        assert "9.2%" in user

    def test_mom_change_omitted_when_not_in_features(self):
        _, user = self.builder.build(SAMPLE_FEATURES)
        assert "Month-on-month" not in user

    def test_supported_versions_constant(self):
        assert "v1" in SUPPORTED_VERSIONS
        assert "v2" in SUPPORTED_VERSIONS
