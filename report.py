"""
report.py - Generate diagnostic reports for sync operations.

Provides human-readable reports that compare Nexudus and Doorflow states,
showing team/group names instead of IDs where available, and previewing
what changes would be made.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from doorflow import DoorflowMember
from nexudus import NexudusMember
from reconcile import Diff

log = logging.getLogger(__name__)


class ReportGenerator:
    """Generate diagnostic sync reports with name resolution."""

    def __init__(self):
        """Load mappings for name resolution."""
        self.mappings = self._load_mappings()
        self.nexudus_team_names = self._build_nexudus_team_names()
        self.doorflow_group_names = self._build_doorflow_group_names()

    def _load_mappings(self) -> dict:
        """Load the mappings.json configuration."""
        mappings_file = os.path.join(os.path.dirname(__file__), "mappings.json")
        try:
            with open(mappings_file, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            log.error("Failed to load mappings from %s: %s", mappings_file, e)
            return {}

    def _build_nexudus_team_names(self) -> dict:
        """Build a mapping of Nexudus team ID -> team name."""
        team_names = {}
        if "team_mappings" in self.mappings:
            for team_id_str, config in self.mappings["team_mappings"].items():
                if isinstance(config, dict) and "name" in config:
                    team_names[int(team_id_str)] = config["name"]
        return team_names

    def _build_doorflow_group_names(self) -> dict:
        """Build a mapping of Doorflow group ID -> group name."""
        group_names = {}
        if "team_mappings" in self.mappings:
            for config in self.mappings["team_mappings"].values():
                if isinstance(config, dict) and "doorflow_group_id" in config:
                    group_id = config["doorflow_group_id"]
                    # Use the team name as the group name for mapped groups
                    if "name" in config:
                        group_names[group_id] = f"{config['name']}"
        # Add basic member group
        if "basic_member" in self.mappings:
            basic = self.mappings["basic_member"]
            if isinstance(basic, dict) and "id" in basic:
                if "name" in basic:
                    group_names[basic["id"]] = basic["name"]
        return group_names

    def _resolve_nexudus_teams(self, team_ids: list[int]) -> str:
        """
        Convert Nexudus team IDs to readable strings.
        Shows names where available, falls back to IDs.
        """
        if not team_ids:
            return "(none)"
        parts = []
        for team_id in sorted(team_ids):
            name = self.nexudus_team_names.get(team_id)
            if name:
                parts.append(f"{name} ({team_id})")
            else:
                parts.append(str(team_id))
        return ", ".join(parts)

    def _resolve_doorflow_groups(self, group_ids: list[int]) -> str:
        """
        Convert Doorflow group IDs to readable strings.
        Shows names where available, falls back to IDs.
        """
        if not group_ids:
            return "(none)"
        parts = []
        for group_id in sorted(group_ids):
            name = self.doorflow_group_names.get(group_id)
            if name:
                parts.append(f"{name} ({group_id})")
            else:
                parts.append(str(group_id))
        return ", ".join(parts)

    def generate(
        self,
        nexudus_members: list[NexudusMember],
        doorflow_members: list[DoorflowMember],
        changes: Diff,
        mode: str = "full",
        since: Optional[datetime] = None,
    ) -> str:
        """
        Generate a comprehensive diagnostic report with a member table.

        Args:
            nexudus_members: Members from Nexudus
            doorflow_members: Members from Doorflow
            diff: Computed differences
            mode: "full" or "fast" sync
            since: For fast sync, the timestamp since which data was fetched

        Returns:
            Formatted report as a string
        """
        lines = []

        # Header
        lines.append("=" * 160)
        lines.append("NEXUDUS-DOORFLOW SYNC REPORT")
        lines.append("=" * 160)
        lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
        lines.append(f"Mode: {'Full Sync' if mode == 'full' else 'Fast Sync'}")
        if since:
            lines.append(f"Since: {since.isoformat()}")
        lines.append("")

        # Summary stats
        lines.append("-" * 160)
        lines.append("SUMMARY")
        lines.append("-" * 160)
        lines.append(f"Nexudus members:       {len(nexudus_members)}")
        lines.append(f"Doorflow members:      {len(doorflow_members)}")
        lines.append(f"Adds:    {len(changes.adds)}")
        lines.append(f"Removes: {len(changes.removes)}")
        lines.append(f"Updates: {len(changes.updates)}")
        lines.append("")

        # Mapping reference section
        lines.extend(self._generate_mappings_section())
        lines.append("")

        # Member comparison table
        lines.append("-" * 160)
        lines.append("MEMBER COMPARISON TABLE")
        lines.append("-" * 160)
        lines.append("")
        lines.append(self._generate_member_table(nexudus_members, doorflow_members, changes))
        lines.append("")

        lines.append("=" * 160)
        return "\n".join(lines)

    def _generate_member_table(
        self,
        nexudus_members: list[NexudusMember],
        doorflow_members: list[DoorflowMember],
        diff: Diff,
    ) -> str:
        """Generate a table comparing all members with per-item lines."""
        nexudus_by_email = {m.email: m for m in nexudus_members}
        doorflow_by_email = {m.email: m for m in doorflow_members}

        adds_set = {m.email for m in diff.adds}
        removes_set = {m.email for m in diff.removes}
        updates_set = {m.email for m in diff.updates}

        all_emails = sorted(set(nexudus_by_email.keys()) | set(doorflow_by_email.keys()))

        lines = []
        header = f"{'Email':<30} | {'Nexudus Team':<30} | {'Doorflow Group':<30} | {'Action':<10}"
        separator = "-" * len(header)
        lines.append(header)
        lines.append(separator)

        for email in all_emails:
            nex_member = nexudus_by_email.get(email)
            df_member = doorflow_by_email.get(email)

            # determine action
            if email in adds_set:
                action = "ADD"
            elif email in removes_set:
                action = "REMOVE"
            elif email in updates_set:
                action = "UPDATE"
            else:
                action = "No Change"

            # resolve item lists
            nex_ids = sorted(nex_member.team_ids) if nex_member else []
            door_ids = []
            if df_member is not None:
                # doorflow.groups can be ints from normal sync path
                # or strings if URL-based results are unparsed.
                for group in df_member.groups:
                    if isinstance(group, int):
                        door_ids.append(group)
                    else:
                        try:
                            door_ids.append(int(str(group).strip()))
                        except ValueError:
                            # fallback keep as text sentinel under None
                            pass

            # build resolved names by id
            # match entries by mapped doorflow group id
            pair_rows = []
            matched = []
            for nid in nex_ids:
                team_config = self.mappings.get("team_mappings", {}).get(str(nid))
                if team_config and isinstance(team_config, dict):
                    did = team_config.get("doorflow_group_id")
                    if did in door_ids:
                        pair_rows.append((self._resolve_nexudus_teams([nid]), self._resolve_doorflow_groups([did])))
                        matched.append(nid)

            only_nex = [nid for nid in nex_ids if nid not in matched]
            only_door = [did for did in door_ids if did not in [self.mappings.get("team_mappings", {}).get(str(nid), {}).get("doorflow_group_id") for nid in matched]]

            for nid in only_nex:
                pair_rows.append((self._resolve_nexudus_teams([nid]), "(none)"))
            for did in only_door:
                pair_rows.append(("(none)", self._resolve_doorflow_groups([did])))

            

            if not pair_rows:
                pair_rows = [("(none)", "(none)")]

            first = True
            for left, right in pair_rows:
                if first:
                    lines.append(f"{email:<30} | {left:<30} | {right:<30} | {action:<10}")
                    first = False
                else:
                    lines.append(f"{'':<30} | {left:<30} | {right:<30} | {'':<10}")
            lines.append(separator)

        return "\n".join(lines)


    def _generate_mappings_section(self) -> list[str]:
        """Generate the mappings reference section."""
        lines = []
        lines.append("-" * 80)
        lines.append("TEAM/GROUP MAPPINGS REFERENCE")
        lines.append("-" * 80)

        if not self.nexudus_team_names:
            lines.append("(No team mappings configured)")
            return lines

        lines.append(f"{'Nexudus Team':<30} | {'ID':<15} | {'Doorflow Group':<30} | {'ID':<5}")
        lines.append("-" * 80)

        if "team_mappings" in self.mappings:
            for team_id_str, config in sorted(
                self.mappings["team_mappings"].items(),
                key=lambda x: int(x[0]),
            ):
                if isinstance(config, dict):
                    team_name = config.get("name", "")
                    team_id = int(team_id_str)
                    doorflow_id = config.get("doorflow_group_id", "")
                    # Get Doorflow group name (which is also the team name in our config)
                    doorflow_group_name = f"{team_name}"
                    lines.append(
                        f"{team_name:<30} | {team_id:<15} | {doorflow_group_name:<30} | {doorflow_id:<5}"
                    )

        # Add basic member
        if "basic_member" in self.mappings:
            basic = self.mappings["basic_member"]
            if isinstance(basic, dict):
                name = basic.get("name", "")
                member_id = basic.get("id", "")
                lines.append(
                    f"{name:<30} | {'(basic)':<15} | {name:<30} | {member_id:<5}"
                )

        return lines
