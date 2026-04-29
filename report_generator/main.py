#!/usr/bin/env python3
"""CLI entry point for the AI Crime Report Generator."""

import argparse
import logging
import sys
from pathlib import Path

from report_generator.loader import load_crime_data
from report_generator.features import extract_features
from report_generator.prompt_builder import PromptBuilder
from report_generator.generator import generate_report
from report_generator.output import save_report


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG if verbose else logging.INFO,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a crime briefing report from a UK police CSV file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python -m report_generator.main data/2026-02-west-yorkshire-street.csv
  python -m report_generator.main data/current.csv --prev data/previous.csv
  python -m report_generator.main data/current.csv --json --verbose
        """,
    )
    parser.add_argument("filepath", type=Path, help="Path to the current period crime CSV.")
    parser.add_argument("--prev", type=Path, default=None, help="Prior period CSV for MoM comparison.")
    parser.add_argument("--prompt-version", choices=["v1", "v2"], default="v2")
    parser.add_argument("--json", action="store_true", help="Also export a JSON report.")
    parser.add_argument("--output-dir", type=Path, default=Path("reports"))
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    _configure_logging(args.verbose)

    print("\n── AI Crime Report Generator " + "─" * 33)

    try:
        df = load_crime_data(args.filepath)
    except (FileNotFoundError, ValueError) as exc:
        print(f"\n  ERROR: {exc}")
        sys.exit(1)

    prev_df = None
    if args.prev:
        try:
            prev_df = load_crime_data(args.prev)
        except (FileNotFoundError, ValueError) as exc:
            print(f"  WARNING: Could not load previous data — skipping. ({exc})")

    features = extract_features(df, prev_df)
    print(f"  Force   : {features['force']}")
    print(f"  Period  : {features['period']}")
    print(f"  Crimes  : {features['total_crimes']:,}")

    builder = PromptBuilder()
    system, user = builder.build(features, version=args.prompt_version)
    print(f"  Prompt  : version={args.prompt_version}")
    print("  API     : calling Gemini...")

    try:
        report = generate_report(system, user)
    except (EnvironmentError, RuntimeError) as exc:
        print(f"\n  ERROR: {exc}")
        sys.exit(1)

    saved = save_report(report, features, output_dir=args.output_dir, json_export=args.json)
    print(f"  Saved   : {saved}")

    print("\n── Report " + "─" * 52)
    print(report)
    print("─" * 62 + "\n")


if __name__ == "__main__":
    main()
