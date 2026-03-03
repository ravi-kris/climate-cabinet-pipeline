from pathlib import Path

import pandas as pd

from climate_cabinet_pipeline.pipeline import PipelinePaths, run_full_pipeline


def test_full_pipeline_builds_expected_outputs(tmp_path: Path) -> None:
    source = tmp_path / "sources"
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"

    source.mkdir(parents=True, exist_ok=True)
    sample = Path("data/sources/campaign_finance_sample.csv")
    sample_data = pd.read_csv(sample)
    sample_data.to_csv(source / "campaign_finance_sample.csv", index=False)

    outputs = run_full_pipeline(
        PipelinePaths(source_dir=source, bronze_dir=bronze, silver_dir=silver, gold_dir=gold)
    )

    assert "bronze" in outputs and outputs["bronze"]
    assert "silver" in outputs and outputs["silver"]
    assert "gold" in outputs and outputs["gold"]

    district_metrics = pd.read_parquet(gold / "district_metrics.parquet")
    assert not district_metrics.empty
    assert {"state", "district", "total_contributions"}.issubset(district_metrics.columns)

    quality_report = (silver / "quality_report.json").read_text(encoding="utf-8")
    assert "valid_records" in quality_report

    scd2 = pd.read_parquet(silver / "donor_candidate_scd2.parquet")
    assert {"relationship_sk", "valid_from", "is_current"}.issubset(scd2.columns)
