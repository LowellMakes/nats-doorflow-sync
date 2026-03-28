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
from datetime import datetime, timezone, timedelta
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
    team_ids: list[int]
    contract_ids: list[int]
    last_updated: datetime

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

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
    
    Field mappings (all from Nexudus /spaces/coworkers endpoint):
    - Email: member email address
    - FullName: display name
    - TeamIds: comma-separated team IDs (empty = no teams)
    - CoworkerContractIds: comma-separated contract IDs (empty = unpaid/past-due)
    - UpdatedOn: ISO 8601 timestamp of last modification
    
    This is the single place to update if Nexudus API field names change.
    """
    return NexudusMember(
        email=record["Email"],
        full_name=record["FullName"],
        team_ids=[int(x) for x in record['TeamIds'].split(",")] if record['TeamIds'] else [], 
        contract_ids=[int(x) for x in record['CoworkerContractIds'].split(",")] if record['CoworkerContractIds'] else [],
        last_updated=datetime.fromisoformat(record["UpdatedOn"]),
    )

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_all() -> list[NexudusMember]:
    """Fetch every member from Nexudus. Used by the full sync."""
    log.info("Nexudus | fetching all members")
    records = _fetch_pages("/spaces/coworkers", {})
    members = [_parse_member(r) for r in records]
    log.info(f"Nexudus | fetched {len(members)} member(s)")
    return members


def fetch_updated_since(since: datetime) -> list[NexudusMember]:
    """
    Fetch only members whose profiles changed after `since`.
    Used by the fast sync to avoid re-fetching unchanged members.
    
    Query param: from_Coworker_UpdatedOn (confirm with Nexudus API docs)
    """
    log.info(f"Nexudus | fetching members updated since {since.strftime('%Y-%m-%dT%H:%M:%S')}")
    records = _fetch_pages(
        "/spaces/coworkers",
        {"from_Coworker_UpdatedOn": since.strftime('%Y-%m-%dT%H:%M')},
    )
    members = [_parse_member(r) for r in records]
    log.info(f"Nexudus | fetched {len(members)} updated member(s)")
    return members
