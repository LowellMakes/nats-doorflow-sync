# Nexudus → Doorflow Sync

This system keeps member access in Doorflow synchronized with their membership and team assignments in Nexudus. It runs as a scheduled background job to detect changes and apply them automatically.

## Architecture Overview

```
nexudus.py ──→ NexudusMember objects ──┐
                                         ├──→ reconcile.py ──→ Diff ──→ doorflow.py
doorflow.py ──→ DoorflowMember objects ─┘
```

### Three Core Modules

**`nexudus.py`** — Nexudus API client
- Fetches members from Nexudus (full or incremental)
- Handles pagination and retries transparently
- Returns plain `NexudusMember` dataclass objects

**`reconcile.py`** — Pure business logic, no API calls
- `_compute_desired()`: Given Nexudus data, what *should* Doorflow look like?
- `_diff()`: Compare desired vs. actual Doorflow state
- `changes()`: Wrap it together and apply protection rules
- **Trivially testable** — just pass in data, get back results

**`doorflow.py`** — Doorflow API client
- Fetches current member state from Doorflow
- Applies computed changes (add, remove, update)
- Handles retries on transient API failures

### Business Rules

1. **Groups mirror teams** — Each member gets:
   - `basic_member` group (baseline access)
   - Their Nexudus teams mapped to Doorflow groups (via `mappings.json`)

2. **Protected groups prevent removal** — Members in `always_include_groups` (like admin users in group 4719) are **never removed from Doorflow**, even if they drop out of Nexudus. This prevents accidental lockouts.

## Usage

```bash
# Delta sync (since last run)
python main.py --fast

# Full sync (all members)
python main.py --full

# Preview changes without applying
python main.py --fast --dry-run

# Generate diagnostic report
python main.py --fast --report
```

## Configuration

See [MAPPINGS.md](MAPPINGS.md) for how to configure team mappings and protection rules.

## Scheduling

**Fast sync** (incremental, ~5 minutes):
```cron
*/5 * * * *  python /opt/sync/main.py --fast
```

**Full sync** (all members, daily):
```
@daily  python /opt/sync/main.py --full
```

## Logs

Logs are written to `doorflow_sync.log` (rotating, max 5 backups at 1MB each).

## Error Handling

- **API retries** — Transient failures are retried with exponential backoff (up to 5 times)
- **Network timeouts** — 30 second timeout per request
- **Process locking** — Only one sync can run at a time; others exit cleanly with a warning
