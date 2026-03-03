from pathlib import Path

import pandas as pd

from climate_cabinet_pipeline.ingestion import map_openfec_records
from climate_cabinet_pipeline.pipeline import PipelinePaths, run_bronze_stage


def test_map_openfec_records_returns_campaign_schema() -> None:
    records = [
        {
            "sub_id": "123",
            "candidate_id": "H0CA12001",
            "candidate_name": "Ana Lopez",
            "contributor_name": "Green Future PAC",
            "entity_type_desc": "PAC",
            "contribution_receipt_amount": 5000,
            "contribution_receipt_date": "2024-05-01",
            "recipient_state": "CA",
            "recipient_candidate_office_district": "12",
        }
    ]

    frame = map_openfec_records(records)

    expected = {
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

    assert expected.issubset(frame.columns)
    assert frame.iloc[0]["filing_id"] == "123"
    assert frame.iloc[0]["state"] == "CA"
    assert frame.iloc[0]["district"] == "12"


def test_hybrid_bronze_combines_local_and_api(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "sources"
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    source.mkdir(parents=True, exist_ok=True)

    local = pd.DataFrame(
        [
            {
                "filing_id": "LOCAL-1",
                "candidate_id": "C1",
                "candidate_name": "Local Candidate",
                "donor_id": "D1",
                "donor_name": "Local Donor",
                "donor_type": "Individual",
                "amount": 100,
                "contribution_date": "2024-01-01",
                "state": "CA",
                "district": "12",
            }
        ]
    )
    local.to_csv(source / "campaign_finance_sample.csv", index=False)

    api_frame = pd.DataFrame(
        [
            {
                "filing_id": "API-1",
                "candidate_id": "C2",
                "candidate_name": "API Candidate",
                "donor_id": "D2",
                "donor_name": "API Donor",
                "donor_type": "PAC",
                "amount": 200,
                "contribution_date": "2024-02-01",
                "state": "NY",
                "district": "10",
            }
        ]
    )

    monkeypatch.setattr(
        "climate_cabinet_pipeline.pipeline.fetch_openfec_contributions",
        lambda *args, **kwargs: api_frame,
    )

    paths = PipelinePaths(source_dir=source, bronze_dir=bronze, silver_dir=silver, gold_dir=gold)
    bronze_path = run_bronze_stage(
        paths,
        ingestion_mode="hybrid",
        openfec_api_key="test-key",
        persist_api_extracts=False,
    )

    result = pd.read_parquet(bronze_path)
    assert len(result) == 2
    assert {"campaign_finance_sample.csv", "openfec_schedule_a.csv"} == set(result["source_file"].unique())
