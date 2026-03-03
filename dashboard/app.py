from __future__ import annotations

import os
from pathlib import Path
from inspect import signature

# Some environments (e.g., sandboxed runners) can trigger verbose Arrow CPU-probe stderr output.
# These are best-effort knobs; if unsupported they are harmless no-ops.
os.environ.setdefault("ARROW_LOG_LEVEL", "ERROR")
os.environ.setdefault("GLOG_minloglevel", "2")

import altair as alt
import pandas as pd
import streamlit as st


def _dataframe_full_width(data, **kwargs):
    """Call `st.dataframe` in a way that's compatible with older Streamlit."""
    params = signature(st.dataframe).parameters
    if "width" in params:
        return st.dataframe(data, width="stretch", **kwargs)
    return st.dataframe(data, use_container_width=True, **kwargs)


def _altair_full_width(chart, **kwargs):
    """Call `st.altair_chart` in a way that's compatible with older Streamlit."""
    params = signature(st.altair_chart).parameters
    if "width" in params:
        return st.altair_chart(chart, width="stretch", **kwargs)
    return st.altair_chart(chart, use_container_width=True, **kwargs)

def main() -> None:
    st.set_page_config(page_title="Climate Cabinet Finance Dashboard", layout="wide")
    st.title("Climate Cabinet: Campaign Finance Explorer")
    st.caption("Gold layer analytics for district competitiveness and legislative briefing")

    GOLD_DIR = Path("data/gold")

    required_files = {
        "District Metrics": GOLD_DIR / "district_metrics.parquet",
        "Donor Breakdown": GOLD_DIR / "donor_breakdown.parquet",
        "Time Series": GOLD_DIR / "contribution_time_series.parquet",
        "Top Donors": GOLD_DIR / "top_donors.parquet",
    }

    missing = [name for name, file in required_files.items() if not file.exists()]
    if missing:
        st.error(
            "Missing Gold outputs. Run `climate-cabinet-pipeline --stage all` first. "
            f"Missing: {', '.join(missing)}"
        )
        st.stop()


    district_metrics = pd.read_parquet(required_files["District Metrics"])
    donor_breakdown = pd.read_parquet(required_files["Donor Breakdown"])
    time_series = pd.read_parquet(required_files["Time Series"])
    top_donors = pd.read_parquet(required_files["Top Donors"])

    states = sorted(district_metrics["state"].unique().tolist())
    selected_states = st.sidebar.multiselect("State", states, default=states)

    filtered_districts = district_metrics[district_metrics["state"].isin(selected_states)]
    all_districts = sorted(filtered_districts["district"].unique().tolist())
    selected_districts = st.sidebar.multiselect("District", all_districts, default=all_districts)

    scope = district_metrics[
        district_metrics["state"].isin(selected_states)
        & district_metrics["district"].isin(selected_districts)
    ]

    donor_scope = donor_breakdown[
        donor_breakdown["state"].isin(selected_states)
        & donor_breakdown["district"].isin(selected_districts)
    ]

    time_scope = time_series[
        time_series["state"].isin(selected_states)
        & time_series["district"].isin(selected_districts)
    ].copy()
    time_scope["contribution_month"] = pd.to_datetime(time_scope["contribution_month"])

    left, mid, right = st.columns(3)
    left.metric("Total Contributions", f"${scope['total_contributions'].sum():,.0f}")
    mid.metric("PAC Contributions", f"${scope['pac_contributions'].sum():,.0f}")
    right.metric(
        "Individual Contributions", f"${scope['individual_contributions'].sum():,.0f}"
    )

    st.subheader("District Funding")
    _dataframe_full_width(
        scope.sort_values("total_contributions", ascending=False),
        hide_index=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("PAC vs Individual Mix")
        mix_chart = (
            alt.Chart(donor_scope)
            .mark_bar()
            .encode(
                x=alt.X("sum(total_amount):Q", title="Contributions (USD)"),
                y=alt.Y("donor_type:N", sort="-x", title="Donor Type"),
                color="donor_type:N",
                tooltip=[
                    "donor_type",
                    alt.Tooltip("sum(total_amount):Q", format=",.0f"),
                ],
            )
        )
        _altair_full_width(mix_chart)

    with col2:
        st.subheader("Funding Trend Over Time")
        trend_chart = (
            alt.Chart(time_scope)
            .mark_line(point=True)
            .encode(
                x=alt.X("contribution_month:T", title="Month"),
                y=alt.Y("sum(total_contributions):Q", title="Contributions (USD)"),
                color="state:N",
                tooltip=[
                    "state",
                    "district",
                    "contribution_month",
                    alt.Tooltip("total_contributions:Q", format=",.0f"),
                ],
            )
        )
        _altair_full_width(trend_chart)

    st.subheader("Top Donors by Candidate")
    candidate_filter = st.selectbox(
        "Candidate",
        sorted(top_donors["candidate_name"].unique().tolist()),
    )
    _dataframe_full_width(
        top_donors[top_donors["candidate_name"] == candidate_filter]
        .sort_values("donor_rank")
        .reset_index(drop=True),
        hide_index=True,
    )


if __name__ == "__main__":
    import threading

    from streamlit.runtime.scriptrunner_utils import script_run_context

    running_in_streamlit = hasattr(
        threading.current_thread(),
        script_run_context.SCRIPT_RUN_CONTEXT_ATTR_NAME,
    )
    if not running_in_streamlit:
        os.execvp("streamlit", ["streamlit", "run", str(Path(__file__).resolve())])

    main()
