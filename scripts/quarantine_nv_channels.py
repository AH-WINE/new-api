#!/usr/bin/env python3
"""Mark known-bad NVIDIA channels as hard-quarantined in a NewAPI SQLite DB.

Default target is the CT112 NewAPI DB layout. The script is idempotent:
- sets channels.status=4 (common.ChannelStatusQuarantined)
- disables all abilities for those channels
- writes remark and other_info.status_reason/status_time for UI clarity
- preserves channel tags so tag grouping and ability tag metadata are not changed
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

QUARANTINED_STATUS = 4
DEFAULT_DB = "/opt/new-api/data/one-api.db"
DEFAULT_CHANNELS: dict[int, tuple[str, str]] = {
    5: ("QUARANTINED_403_AUTH", "Authorization failed; hard-quarantined, do not auto-test or auto-recover"),
    **{
        channel_id: (
            "QUARANTINED_TLS_EOF_V4PRO",
            "TLS EOF/bad record MAC on v4-pro production-shaped traffic; hard-quarantined, do not auto-test or auto-recover",
        )
        for channel_id in [6, 7, 8, 9, 10, 11, 12, 13, 47]
    },
}


def parse_channel_ids(raw: str | None) -> list[int]:
    if not raw:
        return sorted(DEFAULT_CHANNELS)
    ids: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            ids.extend(range(int(start), int(end) + 1))
        else:
            ids.append(int(part))
    return sorted(dict.fromkeys(ids))


def load_other_info(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {"previous_other_info": raw}
    if isinstance(value, dict):
        return value
    return {"previous_other_info": value}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path")
    parser.add_argument("--ids", default=None, help="Channel ids/ranges, e.g. 5-13,47. Defaults to CT112 known-bad NV ids.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned changes without writing")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    ids = parse_channel_ids(args.ids)
    now = int(time.time())
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    rows = con.execute(
        f"SELECT id, name, status, tag, remark, other_info FROM channels WHERE id IN ({','.join('?' for _ in ids)}) ORDER BY id",
        ids,
    ).fetchall()
    found_ids = {int(row["id"]) for row in rows}
    missing = [channel_id for channel_id in ids if channel_id not in found_ids]

    planned: list[dict[str, Any]] = []
    for row in rows:
        channel_id = int(row["id"])
        tag, reason = DEFAULT_CHANNELS.get(
            channel_id,
            ("QUARANTINED_MANUAL", "Hard-quarantined, do not auto-test or auto-recover"),
        )
        other_info = load_other_info(row["other_info"])
        other_info["status_reason"] = reason
        other_info["status_time"] = now
        other_info["quarantine_tag"] = tag
        planned.append(
            {
                "id": channel_id,
                "name": row["name"],
                "old_status": row["status"],
                "new_status": QUARANTINED_STATUS,
                "remark": reason,
                "other_info": json.dumps(other_info, ensure_ascii=False, separators=(",", ":")),
            }
        )

    result = {"dry_run": args.dry_run, "db": str(db_path), "planned": planned, "missing": missing}
    if args.dry_run:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    with con:
        for item in planned:
            con.execute(
                """
                UPDATE channels
                SET status = ?, remark = ?, other_info = ?
                WHERE id = ?
                """,
                (item["new_status"], item["remark"], item["other_info"], item["id"]),
            )
            con.execute("UPDATE abilities SET enabled = 0 WHERE channel_id = ?", (item["id"],))

    result["written"] = len(planned)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
