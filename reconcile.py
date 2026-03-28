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


def load_mappings() -> dict:
    """Load and parse mappings.json configuration.
    
    This is shared across multiple functions to avoid duplicate file I/O.
    """
    mappings_file = os.path.join(os.path.dirname(__file__), "mappings.json")
    try:
        with open(mappings_file, "r") as f:
            return json.load(f)
    except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
        log.error("Failed to load mappings from %s: %s", mappings_file, e)
        raise


@dataclass
class Diff:
    adds: list[DoorflowMember]      # members to create in Doorflow
    removes: list[DoorflowMember]   # members to remove from Doorflow
    updates: list[DoorflowMember]   # members whose groups need updating


def _get_doorflow_groups(
    team_ids: list[int],
    team_mappings: dict,
    basic_member_id: int,
) -> list[int]:
    """
    Translate Nexudus team IDs to Doorflow group IDs.
    
    Returns: [basic_member_id] + [mapped group IDs for teams that have mappings]
    
    Logs warnings for any teams that don't have a mapping in mappings.json.
    Those members just won't get the unmapped team's group in Doorflow.
    """
    # Map available teams to their Doorflow groups
    mapped_groups = [
        team_mappings[str(team_id)]["doorflow_group_id"]
        for team_id in team_ids
        if str(team_id) in team_mappings
    ]
    
    # Log unmapped teams so admins can add them to mappings.json
    for team_id in team_ids:
        if str(team_id) not in team_mappings:
            log.warning(f"Nexudus team {team_id} has no Doorflow mapping")
    
    return [basic_member_id] + mapped_groups


def _compute_desired(members: list[NexudusMember]) -> list[DoorflowMember]:
    """
    Compute the desired Doorflow state from a list of Nexudus members.
    
    Business logic (payment & access):
    - If contract_ids is empty, they owe money → no access in Doorflow
    - If contract_ids is non-empty, they're paid up → grant access:
      - basic_member group (always)
      - + any team-mapped groups (from mappings.json)
    
    This is the "source of truth" calculation. The actual sync compares this
    against current Doorflow state to determine what needs to change.
    """
    config = load_mappings()
    team_mappings = config["team_mappings"]
    basic_member = config["basic_member"]["id"]

    desired = []
    for member in members:
        if not member.contract_ids:
            # Member owes money — no access until resolved.
            continue

        groups = _get_doorflow_groups(member.team_ids, team_mappings, basic_member)
        desired.append(DoorflowMember(email=member.email, groups=groups))
    return desired


def _diff(desired: list[DoorflowMember], actual: list[DoorflowMember]) -> Diff:
    """
    Compare desired Doorflow state against actual Doorflow state.
    
    Returns three lists:
    - adds: Members in desired but not in Doorflow (create them)
    - removes: Members in Doorflow but not in desired (delete them)
    - updates: Members in both but with different groups (change groups)
    
    All comparisons are by email address.
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
    """Compute desired state and compute the diff against actual state.
    
    Members in the cleaner group are protected from removal — even if they're
    not in Nexudus, we won't remove them from Doorflow.
    """
    desired = _compute_desired(nexudus_members)
    diff = _diff(desired, doorflow_members)

    config = load_mappings()
    cleaner_group = config.get("cleaner")

    # Protect members in always_include_groups from removal
    if cleaner_group:
        diff.removes = [
            m for m in diff.removes
            if not any(g == cleaner_group['id'] for g in m.groups)
        ]

    return diff

