#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
probe_missing.py
- DB 기준으로 missing 날짜를 계산하고
- 해당 날짜가 PDF 텍스트에서 어떻게 보이는지(헤더 후보 라인/주변 라인) 찍어주는 디버그 도구

예)
  python probe_missing.py --volume EN_2020 --date 2020-01-04
  python probe_missing.py --volume EN_2020 --all
  python probe_missing.py --only EN --all
  python probe_missing.py --all_volumes --all

기본 경로(프로젝트 루트=EGWDevotionals2013-2025):
  DATA/egw_devotionals.sqlite
  SOURCE/
"""

from __future__ import annotations

import argparse
import calendar
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Optional, Tuple, List

# -----------------------------
# Paths / Constants
# -----------------------------

ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = ROOT / "DATA" / "egw_devotionals.sqlite"
DEFAULT_SOURCE_DIR = ROOT / "SOURCE"

MONTHS_FULL = [
    "January","February","March","April","May","June",
    "July","August","September","October","November","December"
]
MONTH_TO_NUM = {m: i+1 for i, m in enumerate(MONTHS_FULL)}
NUM_TO_MONTH = {i+1: m for i, m in enumerate(MONTHS_FULL)}

MONABBR_TO_NUM = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12
}

# -----------------------------
# DB helpers
# -----------------------------

@dataclass(frozen=True)
class VolumeRow:
    volume_id: str
    lang: str
    year: int
    source_type: str
    source_path: str

def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn

def get_volumes(conn: sqlite3.Connection) -> list[VolumeRow]:
    rows = conn.execute(
        "SELECT volume_id, lang, year, source_type, source_path FROM volumes ORDER BY volume_id"
    ).fetchall()
    out: list[VolumeRow] = []
    for r in rows:
        out.append(VolumeRow(
            volume_id=r["volume_id"],
            lang=r["lang"],
            year=int(r["year"]),
            source_type=r["source_type"],
            source_path=r["source_path"],
        ))
    return out

def get_present_dates(conn: sqlite3.Connection, volume_id: str) -> set[str]:
    rows = conn.execute(
        "SELECT d FROM devotions WHERE volume_id=? ORDER BY d", (volume_id,)
    ).fetchall()
    return {r["d"] for r in rows}

def expected_dates_for_year(y: int) -> list[str]:
    d0 = date(y, 1, 1)
    d1 = date(y, 12, 31)
    out = []
    cur = d0
    while cur <= d1:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out

# -----------------------------
# PDF probing
# -----------------------------

def norm_line(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s).strip()
    return s

NUM_ONLY_RE = re.compile(r"^\d{1,4}$")
BRACKET_PNUM_RE = re.compile(r"^\[\d+\]$")  # [39] 같은 단독 라인

SCRIPTURE_HINT_RE = re.compile(
    r"\b\d?\s*[A-Za-z][A-Za-z’']+(?:\s+[A-Za-z][A-Za-z’']+){0,3}\s+\d+:\d+(?:\s*[-–]\s*\d+)?(?:\s*,\s*\d+)?\b",
    re.IGNORECASE
)
EMDASH_SOURCE_HINT_RE = re.compile(r"—\s*[A-Za-z].{3,160}$")
QUOTE_DASH_HINT_RE = re.compile(r"[“\"].+?[”\"]\s*[—-]\s*.+")
LONG_PROSE_MIN = 80

MONTHS_RE = "(" + "|".join(MONTHS_FULL) + ")"

# v7 헤더 후보 정규식들(빌더와 맞추기)
HDR_TITLE_DATE = re.compile(
    rf"^\s*(?:\[\d+\]\s*)?(?P<title>.+),\s*(?P<month>{MONTHS_RE})\s+(?P<day>\d{{1,2}})\s*(?:\d+)?\s*$",
    re.IGNORECASE
)
HDR_TITLE_DATE_NOCOMMA = re.compile(
    rf"^\s*(?:\[\d+\]\s*)?(?P<title>.+?)\s+(?P<month>{MONTHS_RE})\s+(?P<day>\d{{1,2}})\s*(?:\d+)?\s*$",
    re.IGNORECASE
)
HDR_TITLE_MONTH_ONLY = re.compile(
    rf"^\s*(?:\[\d+\]\s*)?(?P<title>.+),\s*(?P<month>{MONTHS_RE})\s*$",
    re.IGNORECASE
)
HDR_DATE_ONLY = re.compile(
    rf"^\s*(?P<month>{MONTHS_RE})\s+(?P<day>\d{{1,2}})\s*$",
    re.IGNORECASE
)
HDR_TITLE_NUMDATE = re.compile(
    r"^\s*(?:\[\d+\]\s*)?(?P<title>.+),\s*(?P<m>\d{1,2})\s*[\/\-]\s*(?P<day>\d{1,2})\s*(?:\d+)?\s*$"
)
HDR_TITLE_MONABBR = re.compile(
    r"^\s*(?:\[\d+\]\s*)?(?P<title>.+),\s*(?P<mon>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+(?P<day>\d{1,2})\s*(?:\d+)?\s*$",
    re.IGNORECASE
)

def classify_header_line(line: str) -> str:
    """어떤 헤더 패턴에 걸리는지 간단 표시"""
    t = line
    if HDR_TITLE_DATE.match(t): return "HDR_TITLE_DATE (comma)"
    if HDR_TITLE_DATE_NOCOMMA.match(t):
        if re.search(r"\.\s*\.\s*\.", t):
            return "HDR_TITLE_DATE_NOCOMMA (BUT looks like TOC dot-leader)"
        return "HDR_TITLE_DATE_NOCOMMA"
    if HDR_TITLE_MONTH_ONLY.match(t): return "HDR_TITLE_MONTH_ONLY (needs next-day line)"
    if HDR_DATE_ONLY.match(t): return "HDR_DATE_ONLY (title next line)"
    if HDR_TITLE_NUMDATE.match(t): return "HDR_TITLE_NUMDATE"
    if HDR_TITLE_MONABBR.match(t): return "HDR_TITLE_MONABBR"
    return ""

def start_evidence_after_lines(lines: list[str], start_idx: int, lookahead: int = 16) -> Tuple[bool, Optional[str]]:
    """
    build_db.start_evidence_after() 느낌으로,
    start_idx 다음 줄들에서 '본문 시작' 힌트가 보이는지 판단.
    """
    checked = 0
    for j in range(start_idx + 1, min(len(lines), start_idx + 1 + lookahead)):
        t = lines[j]
        if not t:
            continue
        if NUM_ONLY_RE.match(t):
            continue
        if BRACKET_PNUM_RE.match(t):
            continue

        checked += 1
        if SCRIPTURE_HINT_RE.search(t):
            return True, f"scripture_hint: {t}"
        if EMDASH_SOURCE_HINT_RE.search(t):
            return True, f"emdash_source_hint: {t}"
        if QUOTE_DASH_HINT_RE.search(t):
            return True, f"quote_dash_hint: {t}"
        if len(t) >= LONG_PROSE_MIN:
            return True, f"long_prose({len(t)}): {t}"

        if checked >= 10:
            break
    return False, None

def guess_pdf_path_from_db(vol: VolumeRow, source_dir: Path) -> Path:
    """
    volumes.source_path를 우선 사용.
    상대경로/잘못된 경우 SOURCE/EN or SOURCE/KO에서 파일명으로 재시도.
    """
    p = Path(vol.source_path)
    if p.exists():
        return p
    # 상대경로일 수도 있음
    if not p.is_absolute():
        p2 = (ROOT / p).resolve()
        if p2.exists():
            return p2
    # fallback: SOURCE/<LANG>/에서 파일명으로 찾기
    lang_dir = source_dir / ("EN" if vol.lang == "en" else "KO")
    cand = lang_dir / Path(vol.source_path).name
    if cand.exists():
        return cand
    raise FileNotFoundError(f"Cannot resolve source_path for {vol.volume_id}: {vol.source_path}")

# -----------------------------
# Needle helpers (핵심 패치)
# -----------------------------

def needle_regex(needle: str) -> re.Pattern:
    """
    needle 예: 'August 29'
    PDF 텍스트가 'August [249]\\n29'처럼 깨져도 매칭되도록 정규식 생성.
    - month 와 day 사이에 공백/줄바꿈/탭 + [숫자] 토큰이 끼어도 허용
    """
    parts = needle.split()
    if len(parts) == 2 and parts[1].isdigit():
        month, day = parts[0], parts[1]
        pat = rf"{re.escape(month)}\s*(?:\[\d+\]\s*)?{re.escape(day)}\b"
        return re.compile(pat, re.IGNORECASE)

    # 기본: 공백이 여러 개(줄바꿈 포함)여도 매칭
    pat = re.escape(needle).replace(r"\ ", r"\s+")
    return re.compile(pat, re.IGNORECASE)

def find_pages_containing(doc, needle: str) -> list[int]:
    """
    기존: if needle in t
    변경: 정규식으로 검색해서 'August [249]\\n29' 같은 케이스도 잡음
    """
    rx = needle_regex(needle)
    hits = []
    for i in range(len(doc)):
        t = doc[i].get_text("text") or ""
        if rx.search(t):
            hits.append(i)
    return hits

def find_needle_context_index(lines: list[str], rx: re.Pattern) -> Optional[int]:
    """
    라인 단위로 needle을 찾되,
    'August [249]' / '29' 처럼 두 줄로 갈라진 경우를 위해
    현재줄+다음줄을 합쳐서도 검사한다.
    매칭되면 컨텍스트 중심 인덱스를 반환.
    """
    n = len(lines)
    for i in range(n):
        if rx.search(lines[i]):
            return i
        if i + 1 < n:
            combo = f"{lines[i]} {lines[i+1]}"
            if rx.search(combo):
                # 보통 month가 앞줄, day가 뒷줄인 경우가 많으니
                # 컨텍스트는 앞줄(i) 기준으로 잡음(원하면 i+1로 바꿔도 됨)
                return i
    return None

def date_needles(iso: str) -> list[str]:
    y, m, d = [int(x) for x in iso.split("-")]
    month = NUM_TO_MONTH[m]
    # PDF들에서 흔히 보이는 텍스트 형태들을 최대한 넓게
    needles = [
        f"{month} {d}",          # January 4
        f"{month} {d:02d}",      # January 04 (혹시)
        f"{d}/{m}",              # (거의 안 쓰지만 혹시) 4/1
        f"{m}/{d}",              # 1/4
        f"{m:02d}/{d:02d}",      # 01/04
        f"{m}-{d}",              # 1-4
        f"{m:02d}-{d:02d}",      # 01-04
    ]
    # 중복 제거(순서 유지)
    out = []
    seen = set()
    for n in needles:
        if n not in seen:
            out.append(n)
            seen.add(n)
    return out

def print_context(lines: list[str], center: int, radius: int = 12) -> None:
    lo = max(0, center - radius)
    hi = min(len(lines), center + radius + 1)
    for i in range(lo, hi):
        tag = ">>" if i == center else "  "
        cls = classify_header_line(lines[i])
        cls_txt = f"  [{cls}]" if cls else ""
        print(f"{tag} {i:05d}: {lines[i]}{cls_txt}")

def probe_one_date(pdf_path: Path, iso: str, max_pages: int = 6) -> int:
    try:
        import fitz  # PyMuPDF
    except ImportError:
        print("[ERR] PyMuPDF not installed.  pip install pymupdf", file=sys.stderr)
        return 2

    print(f"\n[PROBE] {iso}")
    print(f"[PDF]   {pdf_path}")

    doc = fitz.open(str(pdf_path))

    needles = date_needles(iso)
    # 1) 페이지 히트 수집(needle별)
    page_hits = []
    for nd in needles:
        hits = find_pages_containing(doc, nd)
        if hits:
            page_hits.append((nd, hits))

    if not page_hits:
        print("[HIT]  none (no needle found in raw PDF text)")
        return 0

    # 2) union pages (앞에서부터)
    pages_union: List[int] = []
    seen = set()
    for _, hits in page_hits:
        for h in hits:
            if h not in seen:
                pages_union.append(h)
                seen.add(h)
    pages_union.sort()

    print("[HIT]  needles that matched:")
    for nd, hits in page_hits:
        print(f"  - {nd!r}: pages={len(hits)}  sample={hits[:6]}")

    # 3) 페이지별로 라인 dump + 헤더 후보 표시 + evidence 판단
    shown = 0
    for pi in pages_union:
        if shown >= max_pages:
            break
        shown += 1

        raw = doc[pi].get_text("text") or ""
        raw_lines = [norm_line(x) for x in raw.splitlines()]
        raw_lines = [x for x in raw_lines if x]  # 비어있는 줄 제거

        print(f"\n--- PAGE {pi+1} ------------------------------------------------------------")
        # (a) 우선 page 전체에서 "헤더 후보" 라인들만 먼저 요약
        candidates = []
        for idx, ln in enumerate(raw_lines):
            cls = classify_header_line(ln)
            if cls:
                candidates.append((idx, ln, cls))

        if candidates:
            print(f"[CAND] header-like lines on this page: {len(candidates)}")
            for idx, ln, cls in candidates[:30]:
                ok, why = start_evidence_after_lines(raw_lines, idx, lookahead=16)
                ok_txt = "OK" if ok else "NO"
                why_txt = f" | evidence={why}" if why else ""
                print(f"  - line {idx:05d}: {ln}  [{cls}]  start_evidence={ok_txt}{why_txt}")
        else:
            print("[CAND] none (no line matched header regex family on this page)")

        # (b) needle 위치 근처 컨텍스트
        # 기존: if nd in ln
        # 변경: 정규식(rx) + 2줄 합쳐서도 매칭
        printed_any = False
        for nd in needles:
            rx = needle_regex(nd)
            idx = find_needle_context_index(raw_lines, rx)
            if idx is not None:
                printed_any = True
                ok, why = start_evidence_after_lines(raw_lines, idx, lookahead=16)
                ok_txt = "OK" if ok else "NO"
                print(f"\n[CTX] needle={nd!r} at line {idx:05d}  start_evidence={ok_txt}")
                if why:
                    print(f"      evidence -> {why}")
                print_context(raw_lines, idx, radius=12)
                break

        # (c) 페이지 앞부분 스니펫(원문 확인용)
        snippet = (raw[:1800]).rstrip()
        print("\n[SNIP] first 1800 chars of raw page text:")
        print(snippet)

    return 0

# -----------------------------
# main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Probe missing devotions by date and show PDF text context.")
    ap.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH))
    ap.add_argument("--source_dir", type=str, default=str(DEFAULT_SOURCE_DIR))
    ap.add_argument("--volume", type=str, default="", help="exact volume_id (e.g., EN_2020)")
    ap.add_argument("--only", type=str, default="", help="prefix filter for volume_id (e.g., EN or KO)")
    ap.add_argument("--all_volumes", action="store_true", help="probe all volumes (filtered by --only if set)")
    ap.add_argument("--date", type=str, default="", help="ISO date to probe: YYYY-MM-DD")
    ap.add_argument("--all", action="store_true", help="probe all missing dates for chosen volumes")
    ap.add_argument("--max_pages", type=int, default=6, help="max pages to show per date")
    args = ap.parse_args()

    db_path = Path(args.db)
    source_dir = Path(args.source_dir)

    conn = connect_db(db_path)
    vols = get_volumes(conn)

    # volume selection
    selected: list[VolumeRow] = []
    if args.volume:
        selected = [v for v in vols if v.volume_id == args.volume]
        if not selected:
            print(f"[ERR] volume not found in DB: {args.volume}", file=sys.stderr)
            return 2
    elif args.all_volumes or args.only:
        selected = vols[:]
        if args.only:
            selected = [v for v in selected if v.volume_id.startswith(args.only)]
        if not selected:
            print("[ERR] no volumes selected (check --only filter)", file=sys.stderr)
            return 2
    else:
        print("[ERR] choose one: --volume EN_2020  or  --only EN --all_volumes", file=sys.stderr)
        return 2

    # date selection
    if not args.date and not args.all:
        print("[ERR] choose one: --date YYYY-MM-DD  or  --all (missing dates)", file=sys.stderr)
        return 2

    print(f"[DB] {db_path}")
    if args.volume:
        print(f"[FILTER] volume_id == {args.volume}")
    elif args.only:
        print(f"[FILTER] volume_id startswith {args.only}")
    else:
        print("[FILTER] all volumes")

    for v in selected:
        if v.lang != "en" or v.source_type != "pdf":
            # 이 프로브는 PDF(en)쪽 문제를 겨냥. KO/docx는 missing 거의 없고 의미가 적음.
            # 필요하면 여기 조건을 풀어도 됨.
            if args.volume:
                print(f"[SKIP] {v.volume_id} (lang={v.lang}, type={v.source_type}) - probe is for EN PDF")
            continue

        present = get_present_dates(conn, v.volume_id)
        expected = expected_dates_for_year(v.year)
        missing = [d for d in expected if d not in present]

        if args.date:
            # 단일 date probe
            if not args.date.startswith(f"{v.year}-"):
                if args.volume:
                    print(f"[WARN] {args.date} is not in year {v.year} (still probing if possible)")
            pdf_path = guess_pdf_path_from_db(v, source_dir)
            print(f"\n=== {v.volume_id} ({v.year}) ===")
            return probe_one_date(pdf_path, args.date, max_pages=args.max_pages)

        # --all : missing 전체 probe
        if not missing:
            print(f"\n=== {v.volume_id} ({v.year}) ===")
            print("[OK] no missing dates")
            continue

        pdf_path = guess_pdf_path_from_db(v, source_dir)
        print(f"\n=== {v.volume_id} ({v.year}) ===")
        print(f"[MISSING] {len(missing)} dates")
        # 너무 많으면 앞쪽 몇 개만 우선 찍는 게 좋지만, 여기선 전부 돌려줌(사용자가 Ctrl+C 가능)
        for iso in missing:
            rc = probe_one_date(pdf_path, iso, max_pages=args.max_pages)
            if rc != 0:
                return rc

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
