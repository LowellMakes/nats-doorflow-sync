"""
nexudus.py - Nexudus API client.

Responsibilities:
  - Authenticate with the Nexudus API
  - Fetch all members (full sync)
  - Fetch members updated since a given timestamp (fast sync)
  - Handle pagination transparently
  - Retry on transient failures

Returns plain NexudusMember dataclass objects. No business logic here.
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_log, retry_if_exception_type

log = logging.getLogger(__name__)

NEXUDUS_API_BASE = "https://spaces.nexudus.com/api"
NEXUDUS_USER = os.environ["NEXUDUS_USER"]
NEXUDUS_PASS = os.environ["NEXUDUS_PASS"]


@dataclass
class NexudusMember:
    email: str
    full_name: str
    groups: list[str]           # group names this member belongs to
    has_unpaid_invoice: bool    # True if any invoice is currently unpaid
    last_updated: datetime      # when this member's profile was last changed in Nexudus


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    before=before_log(log, logging.WARNING),
    reraise=True,
)
def _get(endpoint: str, params: dict) -> dict:
    """
    Make a single authenticated GET request to the Nexudus API.
    Retries up to 5 times with exponential backoff on network errors.
    Raises on non-2xx responses.
    """
    url = f"{NEXUDUS_API_BASE}{endpoint}"
    response = requests.get(url, params=params, auth=(NEXUDUS_USER, NEXUDUS_PASS), timeout=30)
    response.raise_for_status()
    return response.json()


def _fetch_pages(endpoint: str, params: dict) -> list[dict]:
    """
    Fetch all pages from a Nexudus endpoint and return a flat list of records.
    Callers never see pagination.
    """
    records = []
    page = 1
    while True:
        data = _get(endpoint, params | {"page": page, "size": 100})
        records.extend(data["Records"])
        log.debug(f"Fetched page {page}/{data['TotalPages']} from {endpoint}")
        if page >= data["TotalPages"]:
            break
        page += 1
    return records


def _parse_member(record: dict[str, Any]) -> NexudusMember:
    """
    Convert a raw Nexudus API record into a NexudusMember dataclass.
    All field name mappings live here — one place to update if the API changes.
    """
    return NexudusMember(
        email=record["Email"],
        full_name=record["FullName"],
        groups=[g["Name"] for g in record.get("Groups", [])], has_unpaid_invoice=record["HasUnpaidInvoice"],   # TODO: confirm exact field name
        last_updated=datetime.fromisoformat(record["UpdatedAt"]),  # TODO: confirm exact field name
    )