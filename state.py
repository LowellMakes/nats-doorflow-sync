"""
state.py - Manages persistent sync state and process locking.

The only state we persist is the timestamp of the last successful sync.
This is stored as a plain ISO 8601 string in a text file so it's human-readable
and easy to manually inspect or reset.

To force a full re-sync from scratch:
    $ echo "1970-01-01T00:00:00+00:00" > /var/lib/doorflow_sync/last_sync.txt
  or simply delete the file (the script will treat a missing file as epoch 0).
"""

import fcntl
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger(__name__)

STATE_FILE = Path("./last_sync.txt")
LOCK_FILE  = Path("./doorflow_sync.lock")

# If no last_sync file exists, use this as the default.
# Results in a full fetch on the very first fast sync run.
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Timestamp
# ---------------------------------------------------------------------------

def read_last_sync() -> datetime:
    """
    Read the timestamp of the last successful sync.
    Returns epoch (1970-01-01) if no state file exists yet,
    which causes the fast sync to behave like a full sync on first run.
    """
    if not STATE_FILE.exists():
        log.warning(f"No state file found at {STATE_FILE}, defaulting to epoch")
        return _EPOCH
    raw = STATE_FILE.read_text().strip()
    return datetime.fromisoformat(raw)


def write_last_sync(timestamp: datetime, buffer_minutes: int = 0) -> None:
    """
    Persist the timestamp of a successful sync.

    buffer_minutes: subtract this many minutes from the timestamp before saving.
    This provides a small overlap window to avoid missing members updated right
    at the boundary between Nexudus's clock and ours.
    """
    adjusted = timestamp - timedelta(minutes=buffer_minutes)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(adjusted.isoformat())
    log.debug(f"Last sync timestamp written: {adjusted.isoformat()}")


# ---------------------------------------------------------------------------
# Process lock
# ---------------------------------------------------------------------------

def acquire_lock():
    """
    Acquire an exclusive process lock to prevent two syncs running at once.
    If the lock is already held, logs a message and exits cleanly.
    The lock is automatically released when the returned file object is closed
    or the process exits — even on a crash.

    Usage:
        lock = state.acquire_lock()
        try:
            ...
        finally:
            lock.close()
    """
    f = open(LOCK_FILE, "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        log.debug("Lock acquired")
        return f
    except OSError:
        log.warning("Another sync is already running — exiting")
        raise SystemExit(0)