"""Compatibility wrapper for generic data-extraction validation.

Direct run:
    python -m pipeline.additions.extraction_validator --consensus input/data_extraction_schema.csv
"""

from __future__ import annotations

import argparse

from pipeline.additions.stats_engine import validate_extraction


def _parse_args() -> argparse.Namespace:
    """human readable hint: keep the older command name while routing to the generic validator."""

    parser = argparse.ArgumentParser(description="Validate dynamic data-extraction outputs.")
    parser.add_argument("--consensus", help="Path to human gold-standard CSV")
    return parser.parse_args()


def main() -> None:
    """human readable hint: run the generic extraction validator from stats_engine."""

    args = _parse_args()
    validate_extraction(args.consensus)


if __name__ == "__main__":
    main()
