"""
main.py - Entry point for Nexudus -> Doorflow sync.

Usage:
    python main.py --fast       # Sync only members updated since last run
    python main.py --full       # Sync all members
    python main.py --fast --dry-run
    python main.py --full --dry-run

Scheduled via:
    cron:    */5 * * * *  python /opt/sync/main.py --fast
    anacron: daily        python /opt/sync/main.py --full
"""

import argparse
import logging
import logging.handlers  # explicitly import the handlers submodule (Python 3.12+)
import sys
from datetime import datetime, timezone

import doorflow
import nexudus
import reconcile
import state
from report import ReportGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            "./doorflow_sync.log",
            maxBytes=1_000_000,   # 1MB
            backupCount=5,
        ),
    ],
)
log = logging.getLogger(__name__)


def run_fast_sync(dry_run: bool) -> None:
    """
    Fetch members updated since the last successful sync and reconcile
    their access in Doorflow.
    """
    last_sync = state.read_last_sync()
    log.info(f"FAST SYNC | fetching members updated since {last_sync.isoformat()}")

    updated_members = nexudus.fetch_updated_since(last_sync)
    log.info(f"FAST SYNC | {len(updated_members)} member(s) to process")

    if not updated_members:
        log.info("FAST SYNC | nothing to do")
        state.write_last_sync(datetime.now(timezone.utc))
        return

    # Fetch current Doorflow state only for the affected members
    emails = [m.email for m in updated_members]
    actual = doorflow.fetch_members(emails)

    changes = reconcile.changes(updated_members, actual)
    log.info(
        f"FAST SYNC | diff: {len(changes.adds)} add(s), "
        f"{len(changes.removes)} remove(s), "
        f"{len(changes.updates)} update(s)"
    )

    doorflow.apply(changes, dry_run=dry_run)

    if not dry_run:
        # Use the latest last_updated from Nexudus data, minus a small buffer,
        # so we don't miss members updated right at the boundary.
        latest = max(m.last_updated for m in updated_members)
        state.write_last_sync(latest, buffer_minutes=2)
        log.info("FAST SYNC | complete")
    else:
        log.info("FAST SYNC | dry-run complete, no changes applied")


def run_full_sync(dry_run: bool) -> None:
    """
    Fetch all members from Nexudus, reconcile against all members in
    Doorflow, and apply any differences.
    """
    log.info("FULL SYNC | fetching all members from Nexudus")
    all_members = nexudus.fetch_all()
    log.info(f"FULL SYNC | {len(all_members)} member(s) fetched from Nexudus")

    log.info("FULL SYNC | fetching all members from Doorflow")
    actual = doorflow.fetch_all()
    log.info(f"FULL SYNC | {len(actual)} member(s) fetched from Doorflow")

    changes = reconcile.changes(all_members, actual)
    log.info(
        f"FULL SYNC | diff: {len(changes.adds)} add(s), "
        f"{len(changes.removes)} remove(s), "
        f"{len(changes.updates)} update(s)"
    )

    doorflow.apply(changes, dry_run=dry_run)

    if not dry_run:
        # Derive timestamp from Nexudus data rather than local clock.
        latest = max(m.last_updated for m in all_members)
        state.write_last_sync(latest, buffer_minutes=2)
        log.info("FULL SYNC | complete")
    else:
        log.info("FULL SYNC | dry-run complete, no changes applied")


def run_report(mode: str) -> None:
    """Generate a diagnostic report comparing Nexudus and Doorflow states."""
    gen = ReportGenerator()
    
    if mode == "full":
        log.info("REPORT | fetching all members from Nexudus")
        nexudus_members = nexudus.fetch_all()
        log.info(f"REPORT | {len(nexudus_members)} member(s) fetched from Nexudus")
        
        log.info("REPORT | fetching all members from Doorflow")
        doorflow_members = doorflow.fetch_all()
        log.info(f"REPORT | {len(doorflow_members)} member(s) fetched from Doorflow")
        
        changes = reconcile.changes(nexudus_members, doorflow_members)
        since = None
    else:
        # Fast mode
        last_sync = state.read_last_sync()
        log.info(f"REPORT | fetching members updated since {last_sync.isoformat()}")
        nexudus_members = nexudus.fetch_updated_since(last_sync)
        log.info(f"REPORT | {len(nexudus_members)} member(s) fetched from Nexudus")
        
        emails = [m.email for m in nexudus_members]
        log.info(f"REPORT | fetching {len(emails)} member(s) from Doorflow")
        doorflow_members = doorflow.fetch_members(emails)
        log.info(f"REPORT | {len(doorflow_members)} member(s) fetched from Doorflow")
        
        changes = reconcile.changes(nexudus_members, doorflow_members)
        since = last_sync
    
    report = gen.generate(
        nexudus_members=nexudus_members,
        doorflow_members=doorflow_members,
        changes=changes,
        mode=mode,
        since=since,
    )
    print(report)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Nexudus members to Doorflow")
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--fast", action="store_true", help="Delta sync (updated members only)")
    mode_group.add_argument("--full", action="store_true", help="Full sync (all members)")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without applying them")
    parser.add_argument("--report", action="store_true", help="Generate diagnostic report instead of syncing")
    args = parser.parse_args()

    lock = state.acquire_lock()
    try:
        if args.report:
            # Report mode: don't apply changes, just generate diagnostics
            mode = "full" if args.full else "fast"
            run_report(mode)
        elif args.full:
            run_full_sync(dry_run=args.dry_run)
        else:
            run_fast_sync(dry_run=args.dry_run)
    except Exception as e:
        log.exception(f"SYNC FAILED | {e}")
        sys.exit(1)
    finally:
        lock.close()


if __name__ == "__main__":
    import logging.handlers  # noqa: needed for RotatingFileHandler at module level
    main()