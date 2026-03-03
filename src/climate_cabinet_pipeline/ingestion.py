from __future__ import annotations

import hashlib
import io
from typing import Iterable, Sequence

import pandas as pd
import requests

OPENFEC_SCHEDULE_A_ENDPOINT = "https://api.open.fec.gov/v1/schedules/schedule_a/"


def _normalize_state(value: object) -> str:
    if value is None:
        return "NA"
    text = str(value).strip().upper()
    return text if len(text) == 2 else "NA"


def _normalize_district(value: object) -> str:
    if value is None:
        return "00"
    text = str(value).strip().upper()
    if not text:
        return "00"
    if text.isdigit():
        return text.zfill(2)
    return text


def _stable_donor_id(name: str, state: str, date: str, amount: object) -> str:
    token = f"{name}|{state}|{date}|{amount}"
    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:12]
    return f"DONOR-{digest}"


def map_openfec_records(records: Iterable[dict]) -> pd.DataFrame:
    mapped = []
    for idx, row in enumerate(records):
        committee = row.get("committee")
        committee_name = committee.get("name") if isinstance(committee, dict) else None

        filing_id = row.get("sub_id") or row.get("transaction_id") or f"fec-{idx}"
        candidate_id = (
            row.get("candidate_id")
            or row.get("recipient_candidate_id")
            or row.get("committee_id")
            or "UNKNOWN_CANDIDATE"
        )
        candidate_name = row.get("candidate_name") or committee_name or "Unknown Candidate"

        donor_name = row.get("contributor_name") or "Unknown Donor"
        donor_type = row.get("entity_type_desc") or row.get("contributor_type") or "OTHER"

        state = _normalize_state(row.get("recipient_state") or row.get("contributor_state"))
        district = _normalize_district(
            row.get("recipient_candidate_office_district")
            or row.get("candidate_office_district")
            or row.get("district")
        )

        contribution_date = row.get("contribution_receipt_date") or row.get("receipt_date")
        amount = row.get("contribution_receipt_amount") or row.get("amount")

        donor_id = row.get("contributor_id")
        if not donor_id:
            donor_id = _stable_donor_id(str(donor_name), state, str(contribution_date), amount)

        mapped.append(
            {
                "filing_id": str(filing_id),
                "candidate_id": str(candidate_id),
                "candidate_name": str(candidate_name),
                "donor_id": str(donor_id),
                "donor_name": str(donor_name),
                "donor_type": str(donor_type),
                "amount": amount,
                "contribution_date": contribution_date,
                "state": state,
                "district": district,
            }
        )

    return pd.DataFrame(mapped)


def fetch_openfec_contributions(
    api_key: str,
    *,
    pages: int = 1,
    per_page: int = 100,
    cycle: int | None = None,
    recipient_state: str | None = None,
    min_date: str | None = None,
    max_date: str | None = None,
    candidate_id: str | None = None,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    params: dict[str, object] = {
        "api_key": api_key,
        "per_page": per_page,
    }

    if cycle is not None:
        params["two_year_transaction_period"] = cycle
    if recipient_state:
        params["recipient_state"] = recipient_state.upper()
    if min_date:
        params["min_date"] = min_date
    if max_date:
        params["max_date"] = max_date
    if candidate_id:
        params["candidate_id"] = candidate_id

    records: list[dict] = []
    page = 1
    while page <= max(1, pages):
        page_params = dict(params)
        page_params["page"] = page

        response = requests.get(OPENFEC_SCHEDULE_A_ENDPOINT, params=page_params, timeout=timeout_seconds)
        response.raise_for_status()
        payload = response.json()

        results = payload.get("results", [])
        if not results:
            break

        records.extend(results)

        pagination = payload.get("pagination", {})
        total_pages = pagination.get("pages")
        if total_pages is not None and page >= int(total_pages):
            break

        page += 1

    return map_openfec_records(records)


def fetch_campaign_csv_from_url(url: str, timeout_seconds: int = 30) -> pd.DataFrame:
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    csv_stream = io.StringIO(response.text)
    frame = pd.read_csv(csv_stream)
    return frame


def merge_api_frames(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    valid_frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not valid_frames:
        return pd.DataFrame()
    return pd.concat(valid_frames, ignore_index=True)
