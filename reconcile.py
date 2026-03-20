"""
reconcile.py - Pure business logic. No API calls.

This module answers two questions:
  1. compute_desired(): Given Nexudus members, what should Doorflow look like?
  2. diff(): Given desired vs actual Doorflow state, what needs to change?

Because there are no API calls here, this entire module is trivially testable
with plain Python data — no mocking required.
"""

from dataclasses import dataclass
import json
import logging
import os

from doorflow import DoorflowMember
from nexudus import NexudusMember

log = logging.getLogger(__name__)


@dataclass
class Diff:
    adds: list[DoorflowMember]      # members to create in Doorflow
    removes: list[DoorflowMember]   # members to remove from Doorflow
    updates: list[DoorflowMember]   # members whose groups need updating


def _compute_desired(members: list[NexudusMember]) -> list[DoorflowMember]:
    """
    Compute the desired Doorflow state from a list of Nexudus members.

    Business rules:
      - If a member has an unpaid invoice, they get no access in Doorflow.
      - Otherwise, their Doorflow groups mirror their Nexudus groups exactly.
    """
    mappings_file = os.path.join(os.path.dirname(__file__), "mappings.json")
    try:
        with open(mappings_file, "r") as f:
            config = json.load(f)
        team_mappings = config["team_mappings"]
        basic_member = config["basic_member"]["id"]
        always_include_groups = config.get("always_include_groups", [])
    except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
        log.error("Failed to load mappings from %s: %s", mappings_file, e)
        raise

    desired = []
    for member in members:
        if not member.contract_ids:
            # Member owes money — no access until resolved.
            continue

        groups = [basic_member]
        for team in member.team_ids:
            try:
                team_config = team_mappings[str(team)]
                groups.append(team_config["doorflow_group_id"])
            except KeyError:
                log.warning(
                    "Nexudus team %s has no Doorflow mapping; "
                    "skipping for member %s",
                    team,
                    member.email,
                )
        groups.extend(always_include_groups)
        desired.append(DoorflowMember(email=member.email, groups=groups))
    return desired


def _diff(desired: list[DoorflowMember], actual: list[DoorflowMember]) -> Diff:
    """
    Compare desired Doorflow state against actual Doorflow state.
    Returns a Diff describing exactly what needs to change.
    """
    desired_by_email = {m.email: m for m in desired}
    actual_by_email  = {m.email: m for m in actual}

    adds    = []
    removes = []
    updates = []

    # Members in desired but not in Doorflow → add
    for email, member in desired_by_email.items():
        if email not in actual_by_email:
            adds.append(member)

    # Members in Doorflow but not in desired → remove
    for email, member in actual_by_email.items():
        if email not in desired_by_email:
            removes.append(member)

    # Members in both → check if groups have changed
    for email, desired_member in desired_by_email.items():
        if email in actual_by_email:
            actual_member = actual_by_email[email]
            if sorted(desired_member.groups) != sorted(actual_member.groups):
                updates.append(desired_member)

    return Diff(adds=adds, removes=removes, updates=updates)


def changes(nexudus_members: list[NexudusMember], doorflow_members: list[DoorflowMember]):
    desired = _compute_desired(nexudus_members)
    changes = _diff(desired, doorflow_members)


    mappings_file = os.path.join(os.path.dirname(__file__), "mappings.json")
    try:
        with open(mappings_file, "r") as f:
            config = json.load(f)
        always_include_groups = config.get("always_include_groups", [])
    except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
        log.error("Failed to load mappings from %s: %s", mappings_file, e)
        raise

    for g in always_include_groups:
        changes.removes = [m for m in changes.removes if g not in m.groups]

    return changes

