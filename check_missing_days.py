#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
check_missing_days.py

EGWDevotionals2013-2025 DB에서 권(volume)별로
연도 기준 365/366일이 모두 존재하는지 체크하고 missing 날짜를 출력한다.

- build_db.py처럼 빌드/파싱을 하지 않음 (DB READ ONLY).
- devotions(volume_id, d)만 사용.

실행:
  python check_missing_days.py
  python check_missing_days.py --db DATA/egw_devotionals.sqlite
  python check_missing_days.py --only EN_
  python check_missing_days.py --only EN_2015
  python check_missing_days.py --show 400
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = ROOT / "DATA" / "egw_devotionals.sqlite"


# -----------------------------
# helpers
# -----------------------------

def year_from_volume_id(volume_id: str) -> int | None:
    # volume_id like EN_2015, KO_2024
    try:
        parts = volume_id.split("_", 1)
        if len(parts) != 2:
            return None
        y = int(parts[1])
        if 1900 <= y <= 2100:
            return y
        return None
    except Exception:
        return None


def is_leap_year(y: int) -> bool:
    return (y % 4 == 0) and ((y % 100 != 0) or (y % 400 == 0))


def iter_dates_of_year(y: int) -> Iterable[str]:
    d0 = date(y, 1, 1)
    d1 = date(y + 1, 1, 1)
    cur = d0
    while cur < d1:
        yield cur.isoformat()  # YYYY-MM-DD
        cur += timedelta(days=1)


def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# -----------------------------
# core
# -----------------------------

@dataclass
class VolumeReport:
    volume_id: str
    year: int
    expected_days: int
    present_days: int
    missing_days: int
    missing_list: list[str]
    extra_dates: list[str]  # dates in DB that are malformed or not that year


def build_report_for_volume(conn: sqlite3.Connection, volume_id: str) -> VolumeReport:
    y = year_from_volume_id(volume_id)
    if y is None:
        raise ValueError(f"Cannot parse year from volume_id: {volume_id}")

    expected = list(iter_dates_of_year(y))
    expected_set = set(expected)

    rows = conn.execute(
        "SELECT d FROM devotions WHERE volume_id=? ORDER BY d",
        (volume_id,),
    ).fetchall()

    present_raw = [r["d"] for r in rows if r["d"]]
    present_set = set(present_raw)

    # missing (based on year calendar)
    missing = sorted(expected_set - present_set)

    # extra: dates that are not in expected_set (includes malformed or other-year)
    extra = sorted([d for d in present_set if d not in expected_set])

    return VolumeReport(
        volume_id=volume_id,
        year=y,
        expected_days=366 if is_leap_year(y) else 365,
        present_days=len(present_set),
        missing_days=len(missing),
        missing_list=missing,
        extra_dates=extra,
    )


def list_volumes(conn: sqlite3.Connection, only_prefix: str | None) -> list[str]:
    rows = conn.execute("SELECT volume_id FROM volumes ORDER BY volume_id").fetchall()
    vids = [r["volume_id"] for r in rows if r["volume_id"]]
    if only_prefix:
        vids = [v for v in vids if v.startswith(only_prefix)]
    return vids


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH), help="Path to sqlite DB")
    ap.add_argument(
        "--only",
        type=str,
        default="",
        help="volume_id prefix filter. Examples: EN_ , KO_ , EN_2015",
    )
    ap.add_argument(
        "--show",
        type=int,
        default=200,
        help="max missing dates to print per volume (rest summarized)",
    )
    args = ap.parse_args()

    db_path = Path(args.db)
    only = args.only.strip() or None
    show_n = max(0, int(args.show))

    try:
        conn = connect_db(db_path)
    except Exception as e:
        print(f"[ERR] {e}", file=sys.stderr)
        return 2

    # sanity: tables existence
    try:
        conn.execute("SELECT 1 FROM devotions LIMIT 1").fetchone()
        conn.execute("SELECT 1 FROM volumes LIMIT 1").fetchone()
    except Exception as e:
        print(f"[ERR] DB schema not found or invalid: {e}", file=sys.stderr)
        return 3

    vols = list_volumes(conn, only)
    if not vols:
        print("[ERR] No volumes found (maybe wrong --only prefix?)", file=sys.stderr)
        return 4

    grand_expected = 0
    grand_present = 0
    grand_missing = 0

    print(f"[DB] {db_path}")
    if only:
        print(f"[FILTER] volume_id startswith: {only}")
    print("")

    for vid in vols:
        try:
            rep = build_report_for_volume(conn, vid)
        except Exception as e:
            print(f"[WARN] {vid}: {e}")
            continue

        grand_expected += rep.expected_days
        grand_present += rep.present_days
        grand_missing += rep.missing_days

        status = "OK" if rep.missing_days == 0 else "MISSING"
        print(f"=== {vid} ({rep.year}) ===")
        print(
            f"[{status}] expected={rep.expected_days}  present={rep.present_days}  missing={rep.missing_days}"
        )

        if rep.extra_dates:
            # show up to 30 extras
            extras_show = rep.extra_dates[:30]
            more = len(rep.extra_dates) - len(extras_show)
            if more > 0:
                print(f"  [EXTRA] {len(rep.extra_dates)} dates not in {rep.year} calendar (showing 30):")
            else:
                print(f"  [EXTRA] {len(rep.extra_dates)} dates not in {rep.year} calendar:")
            print("   - " + ", ".join(extras_show) + (f" ... (+{more})" if more > 0 else ""))

        if rep.missing_days > 0:
            if show_n == 0:
                print("  [MISSING LIST] (suppressed by --show 0)")
            else:
                show = rep.missing_list[:show_n]
                more = len(rep.missing_list) - len(show)
                print(f"  [MISSING LIST] showing {len(show)}" + (f" (+{more} more)" if more > 0 else "") + ":")
                # print in rows for readability
                line = []
                for d in show:
                    line.append(d)
                    if len(line) >= 12:
                        print("   - " + ", ".join(line))
                        line = []
                if line:
                    print("   - " + ", ".join(line))

        print("")

    print("=== SUMMARY ===")
    print(f"volumes={len(vols)}")
    print(f"expected_total={grand_expected}  present_total={grand_present}  missing_total={grand_missing}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
