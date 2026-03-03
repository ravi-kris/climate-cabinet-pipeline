from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import pandas as pd

from .ingestion import fetch_campaign_csv_from_url, fetch_openfec_contributions


REQUIRED_COLUMNS = {
    "filing_id",
    "candidate_id",
    "candidate_name",
    "donor_id",
    "donor_name",
    "donor_type",
    "amount",
    "contribution_date",
    "state",
    "district",
}

COLUMN_ALIASES = {
    "filing_id": ["filing_id", "transaction_id", "sub_id", "record_id", "id"],
    "candidate_id": ["candidate_id", "recipient_candidate_id", "committee_id", "recipient_id"],
    "candidate_name": ["candidate_name", "candidate", "recipient_name", "committee_name"],
    "donor_id": ["donor_id", "contributor_id", "donor_identifier"],
    "donor_name": ["donor_name", "contributor_name", "contributor", "name"],
    "donor_type": ["donor_type", "entity_type_desc", "contributor_type"],
    "amount": ["amount", "contribution_receipt_amount", "contribution_amount"],
    "contribution_date": ["contribution_date", "contribution_receipt_date", "receipt_date", "date"],
    "state": ["state", "recipient_state", "contributor_state"],
    "district": ["district", "recipient_candidate_office_district", "office_district"],
}


def _normalize_donor_type(value: str) -> str:
    if not isinstance(value, str):
        return "UNKNOWN"
    normalized = value.strip().upper()
    if normalized in {"PAC", "POLITICAL ACTION COMMITTEE"}:
        return "PAC"
    if normalized in {"INDIVIDUAL", "PERSON", "CITIZEN"}:
        return "INDIVIDUAL"
    return "OTHER"


def _coerce_campaign_schema(frame: pd.DataFrame, *, source_name: str) -> pd.DataFrame:
    lower_map = {column.lower().strip(): column for column in frame.columns}
    coerced = pd.DataFrame(index=frame.index)
    missing_fields: list[str] = []

    for canonical, aliases in COLUMN_ALIASES.items():
        source_column = None
        for alias in aliases:
            match = lower_map.get(alias.lower().strip())
            if match:
                source_column = match
                break

        if source_column is None:
            missing_fields.append(canonical)
            continue

        coerced[canonical] = frame[source_column]

    if missing_fields:
        raise ValueError(
            f"Source {source_name} missing required mapped fields: {sorted(missing_fields)}"
        )

    return coerced


@dataclass(frozen=True)
class PipelinePaths:
    source_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path

    def ensure(self) -> None:
        for path in [self.source_dir, self.bronze_dir, self.silver_dir, self.gold_dir]:
            path.mkdir(parents=True, exist_ok=True)


def _load_local_source_frames(
    source_dir: Path,
    *,
    excluded_files: Sequence[str] | None = None,
) -> list[tuple[str, pd.DataFrame]]:
    excluded = set(excluded_files or [])
    frames: list[tuple[str, pd.DataFrame]] = []

    for source_file in sorted(source_dir.glob("*.csv")):
        if source_file.name in excluded:
            continue
        frame = pd.read_csv(source_file)
        coerced = _coerce_campaign_schema(frame, source_name=source_file.name)
        frames.append((source_file.name, coerced))

    return frames


def _write_bronze(paths: PipelinePaths, source_frames: Sequence[tuple[str, pd.DataFrame]]) -> Path:
    if not source_frames:
        raise FileNotFoundError(f"No CSV files found in {paths.source_dir}")

    ingestion_ts = pd.Timestamp.utcnow().isoformat()
    merged_frames: list[pd.DataFrame] = []

    for source_name, frame in source_frames:
        missing = REQUIRED_COLUMNS.difference(frame.columns)
        if missing:
            raise ValueError(
                f"Source frame {source_name} is missing required columns: {sorted(missing)}"
            )

        normalized = frame[list(sorted(REQUIRED_COLUMNS))].copy()
        normalized["source_file"] = source_name
        normalized["ingested_at"] = ingestion_ts
        merged_frames.append(normalized)

    bronze_df = pd.concat(merged_frames, ignore_index=True)
    bronze_path = paths.bronze_dir / "contributions_bronze.parquet"
    bronze_df.to_parquet(bronze_path, index=False)
    return bronze_path


def ingest_bronze(paths: PipelinePaths) -> Path:
    paths.ensure()
    source_frames = _load_local_source_frames(paths.source_dir)
    return _write_bronze(paths, source_frames)


def ingest_bronze_api(
    paths: PipelinePaths,
    *,
    openfec_api_key: str | None = None,
    openfec_pages: int = 1,
    openfec_per_page: int = 100,
    openfec_cycle: int | None = None,
    openfec_state: str | None = None,
    openfec_min_date: str | None = None,
    openfec_max_date: str | None = None,
    openfec_candidate_id: str | None = None,
    state_csv_urls: Sequence[str] | None = None,
    include_local_files: bool = False,
    persist_api_extracts: bool = True,
) -> Path:
    paths.ensure()

    api_frames: list[tuple[str, pd.DataFrame]] = []
    state_csv_urls = list(state_csv_urls or [])

    if openfec_api_key:
        openfec_frame = fetch_openfec_contributions(
            openfec_api_key,
            pages=openfec_pages,
            per_page=openfec_per_page,
            cycle=openfec_cycle,
            recipient_state=openfec_state,
            min_date=openfec_min_date,
            max_date=openfec_max_date,
            candidate_id=openfec_candidate_id,
        )

        if not openfec_frame.empty:
            api_frames.append(("openfec_schedule_a.csv", openfec_frame))

    for idx, url in enumerate(state_csv_urls, start=1):
        source_name = f"state_portal_{idx}.csv"
        remote_frame = fetch_campaign_csv_from_url(url)
        coerced = _coerce_campaign_schema(remote_frame, source_name=source_name)
        api_frames.append((source_name, coerced))

    if not api_frames and not include_local_files:
        raise ValueError(
            "No API data was retrieved. Provide OPENFEC_API_KEY or --state-csv-url values, "
            "or use --ingestion-mode file."
        )

    if persist_api_extracts:
        for source_name, frame in api_frames:
            frame.to_csv(paths.source_dir / source_name, index=False)

    source_frames: list[tuple[str, pd.DataFrame]] = []
    if include_local_files:
        excluded = [name for name, _ in api_frames]
        source_frames.extend(_load_local_source_frames(paths.source_dir, excluded_files=excluded))

    source_frames.extend(api_frames)
    return _write_bronze(paths, source_frames)


def run_bronze_stage(
    paths: PipelinePaths,
    *,
    ingestion_mode: str = "file",
    openfec_api_key: str | None = None,
    openfec_pages: int = 1,
    openfec_per_page: int = 100,
    openfec_cycle: int | None = None,
    openfec_state: str | None = None,
    openfec_min_date: str | None = None,
    openfec_max_date: str | None = None,
    openfec_candidate_id: str | None = None,
    state_csv_urls: Sequence[str] | None = None,
    persist_api_extracts: bool = True,
) -> Path:
    mode = ingestion_mode.lower().strip()

    if mode == "file":
        return ingest_bronze(paths)
    if mode == "api":
        return ingest_bronze_api(
            paths,
            openfec_api_key=openfec_api_key,
            openfec_pages=openfec_pages,
            openfec_per_page=openfec_per_page,
            openfec_cycle=openfec_cycle,
            openfec_state=openfec_state,
            openfec_min_date=openfec_min_date,
            openfec_max_date=openfec_max_date,
            openfec_candidate_id=openfec_candidate_id,
            state_csv_urls=state_csv_urls,
            include_local_files=False,
            persist_api_extracts=persist_api_extracts,
        )
    if mode == "hybrid":
        return ingest_bronze_api(
            paths,
            openfec_api_key=openfec_api_key,
            openfec_pages=openfec_pages,
            openfec_per_page=openfec_per_page,
            openfec_cycle=openfec_cycle,
            openfec_state=openfec_state,
            openfec_min_date=openfec_min_date,
            openfec_max_date=openfec_max_date,
            openfec_candidate_id=openfec_candidate_id,
            state_csv_urls=state_csv_urls,
            include_local_files=True,
            persist_api_extracts=persist_api_extracts,
        )

    raise ValueError(f"Unsupported ingestion mode: {ingestion_mode}")


def transform_silver(paths: PipelinePaths) -> Path:
    bronze_path = paths.bronze_dir / "contributions_bronze.parquet"
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze dataset missing at {bronze_path}")

    raw = pd.read_parquet(bronze_path)
    silver = raw.copy()

    silver["filing_id"] = silver["filing_id"].astype(str).str.strip()
    silver["candidate_id"] = silver["candidate_id"].astype(str).str.strip()
    silver["candidate_name"] = silver["candidate_name"].astype(str).str.strip()
    silver["donor_id"] = silver["donor_id"].astype(str).str.strip()
    silver["donor_name"] = silver["donor_name"].astype(str).str.strip()
    silver["state"] = silver["state"].astype(str).str.upper().str.strip()
    silver["district"] = silver["district"].astype(str).str.upper().str.strip()

    silver["amount_usd"] = pd.to_numeric(silver["amount"], errors="coerce")
    silver["contribution_date"] = pd.to_datetime(
        silver["contribution_date"], errors="coerce", utc=True
    ).dt.tz_localize(None)
    silver["election_cycle"] = silver["contribution_date"].dt.year
    silver["donor_type"] = silver["donor_type"].map(_normalize_donor_type)
    silver["pac_flag"] = silver["donor_type"].eq("PAC")

    silver = silver.drop_duplicates(subset=["filing_id"], keep="last")

    key_nulls = silver[
        silver[
            [
                "filing_id",
                "candidate_id",
                "candidate_name",
                "donor_id",
                "donor_name",
                "state",
                "district",
                "contribution_date",
                "amount_usd",
            ]
        ].isnull().any(axis=1)
    ]
    invalid_amounts = silver[silver["amount_usd"] <= 0]
    invalid_state = silver[silver["state"].str.len() != 2]

    invalid_idx = key_nulls.index.union(invalid_amounts.index).union(invalid_state.index)
    exceptions = silver.loc[invalid_idx].copy().sort_index()
    valid = silver.drop(index=invalid_idx).copy()

    quality_report = {
        "total_records": int(len(silver)),
        "valid_records": int(len(valid)),
        "invalid_records": int(len(exceptions)),
        "checks": {
            "key_null_rows": int(len(key_nulls)),
            "non_positive_amount_rows": int(len(invalid_amounts)),
            "invalid_state_rows": int(len(invalid_state)),
            "duplicate_filing_ids_removed": int(len(raw) - len(silver)),
        },
    }

    paths.silver_dir.mkdir(parents=True, exist_ok=True)
    silver_path = paths.silver_dir / "contributions_silver.parquet"
    exceptions_path = paths.silver_dir / "validation_exceptions.csv"
    report_path = paths.silver_dir / "quality_report.json"

    valid.to_parquet(silver_path, index=False)
    exceptions.to_csv(exceptions_path, index=False)
    report_path.write_text(json.dumps(quality_report, indent=2), encoding="utf-8")

    return silver_path


def build_scd2_relationships(paths: PipelinePaths) -> Path:
    silver_path = paths.silver_dir / "contributions_silver.parquet"
    if not silver_path.exists():
        raise FileNotFoundError(f"Silver dataset missing at {silver_path}")

    silver = pd.read_parquet(silver_path)

    per_cycle = (
        silver.groupby(
            ["donor_id", "candidate_id", "candidate_name", "election_cycle", "donor_type"],
            dropna=False,
        )
        .agg(
            total_contributed=("amount_usd", "sum"),
            contribution_count=("filing_id", "count"),
            first_contribution=("contribution_date", "min"),
            last_contribution=("contribution_date", "max"),
        )
        .reset_index()
        .sort_values(["donor_id", "candidate_id", "election_cycle"])
    )

    records: List[Dict[str, object]] = []
    for (_, _), group in per_cycle.groupby(["donor_id", "candidate_id"], sort=False):
        rows = list(group.itertuples(index=False))
        for idx, row in enumerate(rows):
            next_row = rows[idx + 1] if idx + 1 < len(rows) else None
            valid_from = pd.Timestamp(year=int(row.election_cycle), month=1, day=1)
            if next_row is not None:
                valid_to = pd.Timestamp(year=int(next_row.election_cycle), month=1, day=1) - pd.Timedelta(
                    days=1
                )
                is_current = False
            else:
                valid_to = pd.NaT
                is_current = True

            records.append(
                {
                    "relationship_sk": f"{row.donor_id}-{row.candidate_id}-{int(row.election_cycle)}",
                    "donor_id": row.donor_id,
                    "candidate_id": row.candidate_id,
                    "candidate_name": row.candidate_name,
                    "donor_type": row.donor_type,
                    "election_cycle": int(row.election_cycle),
                    "total_contributed": float(row.total_contributed),
                    "contribution_count": int(row.contribution_count),
                    "first_contribution": row.first_contribution,
                    "last_contribution": row.last_contribution,
                    "valid_from": valid_from,
                    "valid_to": valid_to,
                    "is_current": is_current,
                }
            )

    scd2 = pd.DataFrame.from_records(records)
    scd2_path = paths.silver_dir / "donor_candidate_scd2.parquet"
    scd2.to_parquet(scd2_path, index=False)
    return scd2_path


def build_gold(paths: PipelinePaths) -> Iterable[Path]:
    silver_path = paths.silver_dir / "contributions_silver.parquet"
    if not silver_path.exists():
        raise FileNotFoundError(f"Silver dataset missing at {silver_path}")

    paths.gold_dir.mkdir(parents=True, exist_ok=True)
    silver = pd.read_parquet(silver_path)

    district_metrics = (
        silver.groupby(["state", "district", "candidate_id", "candidate_name"], as_index=False)
        .agg(
            total_contributions=("amount_usd", "sum"),
            pac_contributions=("amount_usd", lambda x: x[silver.loc[x.index, "donor_type"] == "PAC"].sum()),
            individual_contributions=(
                "amount_usd",
                lambda x: x[silver.loc[x.index, "donor_type"] == "INDIVIDUAL"].sum(),
            ),
            contribution_count=("filing_id", "count"),
            unique_donors=("donor_id", "nunique"),
        )
        .sort_values("total_contributions", ascending=False)
    )

    donor_breakdown = (
        silver.groupby(["state", "district", "donor_type"], as_index=False)
        .agg(
            total_amount=("amount_usd", "sum"),
            contribution_count=("filing_id", "count"),
            unique_donors=("donor_id", "nunique"),
        )
        .sort_values(["state", "district", "donor_type"])
    )

    month_series = silver.copy()
    month_series["contribution_month"] = month_series["contribution_date"].dt.to_period("M").dt.to_timestamp()
    time_series = (
        month_series.groupby(["contribution_month", "state", "district"], as_index=False)
        .agg(
            total_contributions=("amount_usd", "sum"),
            contribution_count=("filing_id", "count"),
        )
        .sort_values("contribution_month")
    )

    top_donors = (
        silver.groupby(
            ["candidate_id", "candidate_name", "donor_id", "donor_name", "donor_type"],
            as_index=False,
        )
        .agg(total_contributed=("amount_usd", "sum"))
    )
    top_donors["donor_rank"] = (
        top_donors.groupby("candidate_id")["total_contributed"].rank(method="dense", ascending=False).astype(int)
    )
    top_donors = top_donors[top_donors["donor_rank"] <= 5].sort_values(["candidate_id", "donor_rank"])

    district_path = paths.gold_dir / "district_metrics.parquet"
    donor_path = paths.gold_dir / "donor_breakdown.parquet"
    ts_path = paths.gold_dir / "contribution_time_series.parquet"
    top_donor_path = paths.gold_dir / "top_donors.parquet"

    district_metrics.to_parquet(district_path, index=False)
    donor_breakdown.to_parquet(donor_path, index=False)
    time_series.to_parquet(ts_path, index=False)
    top_donors.to_parquet(top_donor_path, index=False)

    return [district_path, donor_path, ts_path, top_donor_path]


def run_full_pipeline(
    paths: PipelinePaths,
    *,
    ingestion_mode: str = "file",
    openfec_api_key: str | None = None,
    openfec_pages: int = 1,
    openfec_per_page: int = 100,
    openfec_cycle: int | None = None,
    openfec_state: str | None = None,
    openfec_min_date: str | None = None,
    openfec_max_date: str | None = None,
    openfec_candidate_id: str | None = None,
    state_csv_urls: Sequence[str] | None = None,
    persist_api_extracts: bool = True,
) -> Dict[str, List[str]]:
    bronze = run_bronze_stage(
        paths,
        ingestion_mode=ingestion_mode,
        openfec_api_key=openfec_api_key,
        openfec_pages=openfec_pages,
        openfec_per_page=openfec_per_page,
        openfec_cycle=openfec_cycle,
        openfec_state=openfec_state,
        openfec_min_date=openfec_min_date,
        openfec_max_date=openfec_max_date,
        openfec_candidate_id=openfec_candidate_id,
        state_csv_urls=state_csv_urls,
        persist_api_extracts=persist_api_extracts,
    )
    silver = transform_silver(paths)
    scd2 = build_scd2_relationships(paths)
    gold = list(build_gold(paths))

    return {
        "bronze": [str(bronze)],
        "silver": [str(silver), str(scd2), str(paths.silver_dir / "quality_report.json")],
        "gold": [str(path) for path in gold],
    }
