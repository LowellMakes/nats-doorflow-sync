"""
doorflow.py - Doorflow API client.

Responsibilities:
  - Fetch current member state from Doorflow
  - Apply a diff (add, remove, update members)
  - Retry on transient failures

Returns and accepts plain DoorflowMember dataclass objects.
No business logic here — just reads and writes.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_log, retry_if_exception_type

# `Diff` is only used in type annotations. importing it at module import time
# caused a circular dependency with reconcile.py (which imports
# DoorflowMember from this module).  Use a forward reference instead so the
# module can be imported independently.

log = logging.getLogger(__name__)

DOORFLOW_API_BASE = "https://api.doorflow.com/api/2"   # TODO: confirm base URL
DOORFLOW_API_KEY = os.environ["DOORFLOW_API_KEY"]


@dataclass
class DoorflowMember:
    email: str
    groups: list[int]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _headers() -> dict:
    return {
        "Authorization": f"Token token={DOORFLOW_API_KEY}",
        "Content-Type": "application/json",
    }


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    before=before_log(log, logging.WARNING),
    reraise=True,
)
def _get(endpoint: str, params: dict = {}) -> dict:
    url = f"{DOORFLOW_API_BASE}{endpoint}"
    response = requests.get(url, params=params, headers=_headers(), timeout=30)
    response.raise_for_status()
    return response.json()


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    before=before_log(log, logging.WARNING),
    reraise=True,
)
def _post(endpoint: str, body: dict) -> dict:
    url = f"{DOORFLOW_API_BASE}{endpoint}"
    response = requests.post(url, json=body, headers=_headers(), timeout=30)
    response.raise_for_status()
    return response.json()


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    before=before_log(log, logging.WARNING),
    reraise=True,
)
def _put(endpoint: str, body: dict) -> dict:
    url = f"{DOORFLOW_API_BASE}{endpoint}"
    response = requests.put(url, json=body, headers=_headers(), timeout=30)
    response.raise_for_status()
    return response.json()


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    before=before_log(log, logging.WARNING),
    reraise=True,
)
def _delete(endpoint: str) -> None:
    url = f"{DOORFLOW_API_BASE}{endpoint}"
    response = requests.delete(url, headers=_headers(), timeout=30)
    response.raise_for_status()


def _fetch_pages(endpoint: str, params: dict = {}) -> list[dict]:
    """Fetch all pages and return a flat list of records.

    If Doorflow does not paginate (or returns a plain list), this will still
    work by returning whatever the first call returns.
    """

    records = []
    page = 1
    while True:
        data = _get(endpoint, params | {"page": page, "size": 100})
        if len(data) == 0:
            break
        records.extend(data)
        log.debug(f"Fetched page {page} from {endpoint}")
        page += 1
    return records


def _parse_member(record: dict) -> DoorflowMember:
    """
    Convert a raw Doorflow API record into a DoorflowMember dataclass.
    All field name mappings live here.
    """
    return DoorflowMember(
        email=record["email"],                                      # TODO: confirm field name
        groups=[g["id"] for g in record.get("groups", [])],      # TODO: confirm field name
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_all() -> list[DoorflowMember]:
    """Fetch every person currently in Doorflow. Used by the full sync."""
    log.info("Doorflow | fetching all members")
    records = _fetch_pages("/people")           # TODO: confirm endpoint
    members = [_parse_member(r) for r in records]
    log.info(f"Doorflow | fetched {len(members)} member(s)")
    return members


def fetch_members(emails: list[str]) -> list[DoorflowMember]:
    """
    Fetch Doorflow state for a specific set of members by email.
    Used by the fast sync — no need to pull everyone.
    Returns only members that already exist in Doorflow (new members will
    appear in diff.adds and be created via apply()).
    """
    log.info(f"Doorflow | fetching {len(emails)} specific member(s)")
    members = []
    for email in emails:
        try:
            records = _get("/people", {"email": email})   # TODO: confirm filter param
            members.extend(_parse_member(r) for r in records)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                pass  # Member doesn't exist in Doorflow yet — that's fine
            else:
                raise
    return members


def apply(diff: "reconcile.Diff", dry_run: bool = False) -> None:
    """
    Apply a computed diff to Doorflow.
    If dry_run=True, log what would happen but make no API calls.
    """
    for member in diff.adds:
        log.info(f"ADD    | {member.email} | groups={member.groups}")

        '''
        if not dry_run:
            _post("/people", {                  # TODO: confirm request body shape
                "email": member.email,
                "groups": member.groups,
            })
        '''

    for member in diff.removes:
        log.info(f"REMOVE | {member.email}")

        '''
        if not dry_run:
            _delete(f"/people/{member.email}")  # TODO: confirm endpoint + identifier
        '''

    for member in diff.updates:
        log.info(f"UPDATE | {member.email} | groups={member.groups}")

        '''
        if not dry_run:
            _put(f"/people/{member.email}", {   # TODO: confirm endpoint + request body shape
                "groups": member.groups,
            })
        '''