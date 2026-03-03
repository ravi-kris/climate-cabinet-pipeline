from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .pipeline import (
    PipelinePaths,
    build_gold,
    build_scd2_relationships,
    run_bronze_stage,
    run_full_pipeline,
    transform_silver,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Climate Cabinet campaign finance pipeline")
    parser.add_argument(
        "--stage",
        default="all",
        choices=["all", "bronze", "silver", "scd2", "gold"],
        help="Pipeline stage to run",
    )
    parser.add_argument(
        "--ingestion-mode",
        default="file",
        choices=["file", "api", "hybrid"],
        help="Bronze ingestion mode: local files, API only, or both.",
    )
    parser.add_argument("--source-dir", default="data/sources")
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--gold-dir", default="data/gold")

    parser.add_argument("--openfec-api-key", default=None)
    parser.add_argument("--openfec-pages", type=int, default=1)
    parser.add_argument("--openfec-per-page", type=int, default=100)
    parser.add_argument("--openfec-cycle", type=int, default=None)
    parser.add_argument("--openfec-state", default=None)
    parser.add_argument("--openfec-min-date", default=None)
    parser.add_argument("--openfec-max-date", default=None)
    parser.add_argument("--openfec-candidate-id", default=None)

    parser.add_argument(
        "--state-csv-url",
        action="append",
        default=[],
        help="Remote CSV URL in campaign finance shape. Repeat for multiple URLs.",
    )
    parser.add_argument(
        "--persist-api-extracts",
        action="store_true",
        help="Persist API extracts into source-dir as CSV files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = PipelinePaths(
        source_dir=Path(args.source_dir),
        bronze_dir=Path(args.bronze_dir),
        silver_dir=Path(args.silver_dir),
        gold_dir=Path(args.gold_dir),
    )

    openfec_api_key = args.openfec_api_key or os.getenv("OPENFEC_API_KEY")

    try:
        if args.stage == "all":
            summary = run_full_pipeline(
                paths,
                ingestion_mode=args.ingestion_mode,
                openfec_api_key=openfec_api_key,
                openfec_pages=args.openfec_pages,
                openfec_per_page=args.openfec_per_page,
                openfec_cycle=args.openfec_cycle,
                openfec_state=args.openfec_state,
                openfec_min_date=args.openfec_min_date,
                openfec_max_date=args.openfec_max_date,
                openfec_candidate_id=args.openfec_candidate_id,
                state_csv_urls=args.state_csv_url,
                persist_api_extracts=args.persist_api_extracts,
            )
        elif args.stage == "bronze":
            summary = {
                "bronze": [
                    str(
                        run_bronze_stage(
                            paths,
                            ingestion_mode=args.ingestion_mode,
                            openfec_api_key=openfec_api_key,
                            openfec_pages=args.openfec_pages,
                            openfec_per_page=args.openfec_per_page,
                            openfec_cycle=args.openfec_cycle,
                            openfec_state=args.openfec_state,
                            openfec_min_date=args.openfec_min_date,
                            openfec_max_date=args.openfec_max_date,
                            openfec_candidate_id=args.openfec_candidate_id,
                            state_csv_urls=args.state_csv_url,
                            persist_api_extracts=args.persist_api_extracts,
                        )
                    )
                ]
            }
        elif args.stage == "silver":
            summary = {"silver": [str(transform_silver(paths))]}
        elif args.stage == "scd2":
            summary = {"silver": [str(build_scd2_relationships(paths))]}
        else:
            summary = {"gold": [str(path) for path in build_gold(paths)]}
    except (ValueError, FileNotFoundError) as exc:
        raise SystemExit(f"Error: {exc}") from exc

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
