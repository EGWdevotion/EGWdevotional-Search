
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EGWDevotionals2013-2025 통합 DB 빌더 (원샷 복붙 최종본 - v8.2+)

핵심 기능:
  ✅ PDF 헤더 파싱(예외 케이스 포함: split header Month -> [page] -> day)
  ✅ [249] 같은 브라켓 페이지번호 라인 스킵
  ✅ continuation(다음 페이지 상단에 같은 날짜 헤더 반복) dedup 처리
  ✅ citation_index 구축
  ✅ FTS5 (segments_fts) 구축
  ✅ 윤년은 "원본에 실제 존재하는 날짜만" DB에 들어가게 됨 (best_by_date + 달력 유효성 검사)

주의:
  - EN: pdf/docx 모두 처리
  - KO: docx 처리
"""

from __future__ import annotations

print(">>> USING BUILD VERSION v8.2 FINAL <<<")

import argparse
import datetime
import hashlib
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional


# -----------------------------
# Paths / Constants
# -----------------------------

ROOT = Path(__file__).resolve().parent
DEFAULT_SOURCE_DIR = ROOT / "SOURCE"
DEFAULT_DATA_DIR = ROOT / "DATA"
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "egw_devotionals.sqlite"
DEFAULT_ABBR_MAP = DEFAULT_SOURCE_DIR / "abbr_map.txt"


# -----------------------------
# Models
# -----------------------------

@dataclass(frozen=True)
class VolumeSpec:
    volume_id: str
    lang: str
    title: str
    year: int
    source_type: str
    source_path: Path

@dataclass
class DevotionRecord:
    volume_id: str
    lang: str
    d: str
    title: str
    body: str
    source_ref: str

@dataclass
class CitationHit:
    segment_pk: int
    raw: str
    norm_key: str
    source_lang: str
    confidence: float


# -----------------------------
# Utilities
# -----------------------------

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def load_abbr_map(abbr_map_path: Path) -> dict[str, str]:
    if not abbr_map_path.exists():
        raise FileNotFoundError(f"abbr_map not found: {abbr_map_path}")

    mp: dict[str, str] = {}
    for line in abbr_map_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        mp[k.strip()] = v.strip()
    return mp

def normalize_spaces(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def guess_year_from_filename(p: Path) -> Optional[int]:
    m = re.search(r"(20\d{2})", p.name)
    return int(m.group(1)) if m else None

def year_from_volume_id(volume_id: str) -> int:
    m = re.search(r"(20\d{2})", volume_id)
    return int(m.group(1)) if m else 1900

def safe_title_from_filename(p: Path) -> str:
    stem = p.stem
    stem = re.sub(r"[_\-]+", " ", stem).strip()
    return stem


# -----------------------------
# Citation detection / normalization
# -----------------------------

class CitationDetector:
    MONTHS = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12
    }

    def __init__(self, abbr_map: dict[str, str]):
        self.abbr_map = abbr_map

        self.wrapper_patterns: list[re.Pattern] = [
            re.compile(r"『(?P<inner>[^』]{2,200})』"),
            re.compile(r"\((?P<inner>[^)]{2,200})\)"),
            re.compile(r"<(?P<inner>[^>]{2,200})>"),
        ]

        self.core_pat = re.compile(
            r"(?P<abbr>[A-Za-z]{2,10})\s*(?P<loc>\d{1,4}(?:[.:]\d{1,4})?)"
        )

        self.work_pages_pat = re.compile(
            r"^(?P<work>[^,]{2,160}),\s*(?P<pages>\d{1,4}(?:\s*,\s*\d{1,4}){0,12})\.?$"
        )
        self.work_date_pat = re.compile(
            r"^(?P<work>[^,]{2,160}),\s*(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)\s+"
            r"(?P<day>\d{1,2}),\s*(?P<year>\d{4})\.?$"
        )

    def normalize_key_abbr_loc(self, abbr: str, loc: str) -> str:
        book = self.abbr_map.get(abbr, abbr)
        loc2 = loc.replace(":", ".")
        return f"{book}|{loc2}"

    def normalize_key_work_pages(self, work: str, pages: str) -> str:
        work = normalize_spaces(work)
        pages = normalize_spaces(pages)
        pages = pages.replace(" ,", ",").replace(", ", ",")
        return f"{work}|{pages}"

    def normalize_key_work_date(self, work: str, month: str, day: str, year: str) -> str:
        work = normalize_spaces(work)
        m = self.MONTHS.get(month)
        if not m:
            return f"{work}|{month} {day}, {year}"
        iso = f"{int(year):04d}-{m:02d}-{int(day):02d}"
        return f"{work}|{iso}"

    def detect_in_text(self, text: str, segment_pk: int) -> list[CitationHit]:
        hits: list[CitationHit] = []

        for wp in self.wrapper_patterns:
            for m in wp.finditer(text):
                raw = m.group(0)
                inner = m.group("inner")
                for cm in self.core_pat.finditer(inner):
                    hits.append(CitationHit(
                        segment_pk=segment_pk,
                        raw=raw,
                        norm_key=self.normalize_key_abbr_loc(cm.group("abbr"), cm.group("loc")),
                        source_lang="en",
                        confidence=0.85
                    ))

        t = text.strip()
        dash_pos = t.rfind("—")
        if dash_pos != -1 and dash_pos >= max(0, len(t) - 280):
            tail = t[dash_pos + 1:].strip()
            rawdash = "—" + tail

            md = self.work_date_pat.match(tail)
            if md:
                hits.append(CitationHit(
                    segment_pk=segment_pk,
                    raw=rawdash,
                    norm_key=self.normalize_key_work_date(
                        md.group("work"), md.group("month"), md.group("day"), md.group("year")
                    ),
                    source_lang="en",
                    confidence=0.92
                ))
            else:
                mp = self.work_pages_pat.match(tail)
                if mp:
                    hits.append(CitationHit(
                        segment_pk=segment_pk,
                        raw=rawdash,
                        norm_key=self.normalize_key_work_pages(mp.group("work"), mp.group("pages")),
                        source_lang="en",
                        confidence=0.90
                    ))
        return hits


# -----------------------------
# DOCX Parser (EN + KO)
# -----------------------------

def parse_docx_to_devotions(path: Path, volume_id: str, lang: str) -> Iterator[DevotionRecord]:
    try:
        import docx  # python-docx
    except ImportError as e:
        raise RuntimeError("python-docx not installed. Install: pip install python-docx") from e

    doc = docx.Document(str(path))

    paras: list[str] = []
    for p in doc.paragraphs:
        t = normalize_spaces(p.text or "")
        if t:
            paras.append(t)

    y = year_from_volume_id(volume_id)

    month_names = "January|February|March|April|May|June|July|August|September|October|November|December"
    en_date_pat = re.compile(rf"^(?P<month>{month_names})\s+(?P<day>\d{{1,2}})$")
    ko_date_pat = re.compile(r"^(?:\d{4}\s*년\s*)?(?P<m>\d{1,2})\s*월\s*(?P<day>\d{1,2})\s*일")

    def is_date_line(s: str) -> bool:
        if lang == "ko":
            return bool(ko_date_pat.match(s))
        return bool(en_date_pat.match(s))

    def parse_iso_date(s: str) -> str:
        if lang == "ko":
            mm = ko_date_pat.match(s)
            mnum = int(mm.group("m"))
            dnum = int(mm.group("day"))
            return f"{y:04d}-{mnum:02d}-{dnum:02d}"

        mm = en_date_pat.match(s)
        month_str = mm.group("month")
        dnum = int(mm.group("day"))
        mnum = CitationDetector.MONTHS[month_str]
        return f"{y:04d}-{mnum:02d}-{dnum:02d}"

    i = 0
    while i < len(paras):
        if not is_date_line(paras[i]):
            i += 1
            continue

        iso_date = parse_iso_date(paras[i])

        i += 1
        while i < len(paras) and not paras[i]:
            i += 1
        title = paras[i] if i < len(paras) else ""
        i += 1

        while i < len(paras) and not paras[i]:
            i += 1
        key_verse = paras[i] if i < len(paras) else ""
        i += 1

        body_parts: list[str] = []
        if key_verse:
            body_parts.append(key_verse)

        while i < len(paras) and not is_date_line(paras[i]):
            line = paras[i]
            dash_pos = line.rfind("—")
            if dash_pos > 0 and dash_pos >= len(line) - 280:
                main = line[:dash_pos].rstrip()
                tail = line[dash_pos:].strip()
                if main:
                    body_parts.append(main)
                body_parts.append(tail)
            else:
                body_parts.append(line)
            i += 1

        body = "\n\n".join([x for x in body_parts if x]).strip()

        yield DevotionRecord(
            volume_id=volume_id,
            lang=lang,
            d=iso_date,
            title=title.strip(),
            body=body,
            source_ref=f"docx:{path.name}",
        )


# -----------------------------
# PDF Parser (EN) - v8.2+
# -----------------------------

def parse_pdf_to_devotions(path: Path, volume_id: str, lang: str, debug_headers: bool = False) -> Iterator[DevotionRecord]:
    """
    PDF -> DevotionRecord yield

    ✅ 핵심 해결:
      - split header: "Title, Month" 다음 줄들에서 [226] 같은 잡음 스킵 후 day 찾기
      - continuation(다음 페이지 상단에 동일 날짜 헤더가 반복되는 경우) dedup 해서 body가 잘리는 문제 해결
    """
    try:
        import fitz  # PyMuPDF
    except ImportError as e:
        raise RuntimeError("PDF parsing requires PyMuPDF. Install: pip install pymupdf") from e

    if lang != "en":
        raise NotImplementedError("PDF parsing currently implemented for EN only.")

    doc = fitz.open(str(path))
    year = year_from_volume_id(volume_id)

    MONTHS_FULL = [
        "January","February","March","April","May","June",
        "July","August","September","October","November","December"
    ]
    MONTHS = "(" + "|".join(MONTHS_FULL) + ")"
    month_to_num = CitationDetector.MONTHS

    def norm(s: str) -> str:
        s = (s or "").replace("\u00a0", " ")
        s = re.sub(r"[ \t]+", " ", s).strip()
        return s

    NUM_ONLY_RE = re.compile(r"^\d{1,4}$")
    BRACKET_PNUM_RE = re.compile(r"^\[\d+\]$")
    DOT_LEADER_RE = re.compile(r"\.\s*\.\s*\.")

    SCRIPTURE_HINT_RE = re.compile(
        r"\b\d?\s*[A-Za-z][A-Za-z’']+(?:\s+[A-Za-z][A-Za-z’']+){0,3}\s+\d+:\d+(?:\s*[-–]\s*\d+)?(?:\s*,\s*\d+)?\b",
        re.IGNORECASE
    )
    EMDASH_SOURCE_HINT_RE = re.compile(r"—\s*[A-Za-z].{3,160}$")
    QUOTE_DASH_HINT_RE = re.compile(r"[“\"].+?[”\"]\s*[—-]\s*.+")
    LONG_PROSE_MIN = 80

    HDR_TITLE_DATE = re.compile(
        rf"^\s*(?:\[\d+\]\s*)?(?P<title>.+),\s*(?P<month>{MONTHS})\s+(?P<day>\d{{1,2}})\s*(?:\d+)?\s*$",
        re.IGNORECASE
    )
    HDR_TITLE_DATE_NOCOMMA = re.compile(
        rf"^\s*(?:\[\d+\]\s*)?(?P<title>.+?)\s+(?P<month>{MONTHS})\s+(?P<day>\d{{1,2}})\s*(?:\d+)?\s*$",
        re.IGNORECASE
    )
    HDR_TITLE_MONTH_ONLY = re.compile(
        rf"^\s*(?:\[\d+\]\s*)?(?P<title>.+),\s*(?P<month>{MONTHS})\s*$",
        re.IGNORECASE
    )
    HDR_DATE_ONLY = re.compile(
        rf"^\s*(?P<month>{MONTHS})\s+(?P<day>\d{{1,2}})\s*$",
        re.IGNORECASE
    )
    HDR_TITLE_NUMDATE = re.compile(
        r"^\s*(?:\[\d+\]\s*)?(?P<title>.+),\s*(?P<m>\d{1,2})\s*[\/\-]\s*(?P<day>\d{1,2})\s*(?:\d+)?\s*$"
    )
    HDR_TITLE_MONABBR = re.compile(
        r"^\s*(?:\[\d+\]\s*)?(?P<title>.+),\s*(?P<mon>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+(?P<day>\d{1,2})\s*(?:\d+)?\s*$",
        re.IGNORECASE
    )
    monabbr_to_num = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12
    }

    # -------------------------------------------------
    # lines: (text, page_no)
    # -------------------------------------------------
    lines: list[tuple[str, int]] = []
    for pi in range(len(doc)):
        txt = doc[pi].get_text("text") or ""
        for raw in txt.splitlines():
            t = norm(raw)
            if t:
                lines.append((t, pi + 1))

    def next_nonempty_idx(i: int) -> int:
        j = i
        while j < len(lines):
            if lines[j][0]:
                return j
            j += 1
        return len(lines)

    def find_next_day(i: int, lookahead: int = 8) -> tuple[int, int]:
        """Look ahead for a day number (1-31), skipping bracket/page-number noise."""
        for j in range(i, min(len(lines), i + lookahead)):
            t, _ = lines[j]
            if not t:
                continue
            if BRACKET_PNUM_RE.match(t):
                continue
            # 226 같은 쪽수(3~4자리) 스킵
            if re.fullmatch(r"\d{3,4}", t):
                continue
            if re.fullmatch(r"\d{1,2}", t):
                dv = int(t)
                if 1 <= dv <= 31:
                    return j, dv
        return -1, -1

    def find_next_title(i: int, lookahead: int = 8) -> tuple[int, str]:
        """Look ahead for a plausible title line, skipping bracket/page-number noise."""
        for j in range(i, min(len(lines), i + lookahead)):
            t, _ = lines[j]
            if not t:
                continue
            if BRACKET_PNUM_RE.match(t):
                continue
            if re.fullmatch(r"\d{1,4}", t):
                continue
            return j, t
        return -1, ""

    def start_evidence_after(i: int) -> bool:
        """
        헤더 다음 줄들에서 '본문 시작' 힌트를 찾는다.
        - 짧은 devotion도 통과하도록 완화
        - 페이지 범위를 p0+2까지 허용(continuation 대비)
        """
        _t0, p0 = lines[i]
        last_page_no = doc.page_count

        # 문서 끝쪽은 evidence 약해도 허용
        if p0 >= last_page_no:
            return True

        checked = 0
        prose_chars = 0
        consecutive_letter_lines = 0

        j = i + 1
        while j < len(lines):
            t, pno = lines[j]

            # p0+2페이지까지만
            if pno > p0 + 2:
                break

            if NUM_ONLY_RE.match(t) or BRACKET_PNUM_RE.match(t):
                j += 1
                continue

            checked += 1

            if DOT_LEADER_RE.search(t):
                consecutive_letter_lines = 0
                j += 1
                continue

            if SCRIPTURE_HINT_RE.search(t):
                return True

            if EMDASH_SOURCE_HINT_RE.search(t) or QUOTE_DASH_HINT_RE.search(t):
                return True

            if len(t) >= LONG_PROSE_MIN:
                return True

            if re.search(r"[A-Za-z]", t):
                prose_chars += len(t)
                consecutive_letter_lines += 1
            else:
                consecutive_letter_lines = 0

            if consecutive_letter_lines >= 3:
                return True

            if prose_chars >= 120:
                return True

            if checked >= 40:
                break

            j += 1

        return False

    headers: list[tuple[int, str, str, int, int]] = []

    i = 0
    while i < len(lines):
        text, page_no = lines[i]

        # A) Title, Month Day
        m1 = HDR_TITLE_DATE.match(text)
        if m1:
            title = m1.group("title").strip()
            month = m1.group("month")
            day = int(m1.group("day"))
            ok = (1 <= day <= 31 and start_evidence_after(i))
            if ok:
                headers.append((i, title, month, day, page_no))
            elif debug_headers:
                print(f"[HDR_DROP] {volume_id} p{page_no} | A | {title} , {month} {day} | evidence=NO")
            i += 1
            continue

        # H) Title Month Day (no comma)
        m1b = HDR_TITLE_DATE_NOCOMMA.match(text)
        if m1b:
            if DOT_LEADER_RE.search(text):
                i += 1
                continue
            title = m1b.group("title").strip()
            month = m1b.group("month")
            day = int(m1b.group("day"))
            ok = (1 <= day <= 31 and start_evidence_after(i))
            if ok:
                headers.append((i, title, month, day, page_no))
            elif debug_headers:
                print(f"[HDR_DROP] {volume_id} p{page_no} | H | {title} {month} {day} | evidence=NO")
            i += 1
            continue

        # D) split header: Title, Month  + 다음 줄 day (중간 [226] 스킵)
        m2 = HDR_TITLE_MONTH_ONLY.match(text)
        if m2:
            j, day = find_next_day(i + 1, lookahead=8)
            if j != -1:
                title = m2.group("title").strip()
                month = m2.group("month")
                ok = start_evidence_after(j)
                if ok:
                    headers.append((i, title, month, day, page_no))
                elif debug_headers:
                    print(f"[HDR_DROP] {volume_id} p{page_no} | D | {title} , {month} / {day} | evidence=NO")
                i = j + 1
                continue

        # C) Date-only then title next line
        m3 = HDR_DATE_ONLY.match(text)
        if m3:
            j, title_line = find_next_title(i + 1, lookahead=8)
            if j != -1 and title_line and len(title_line) >= 6:
                month = m3.group("month")
                day = int(m3.group("day"))
                ok = (1 <= day <= 31 and start_evidence_after(j))
                if ok:
                    headers.append((i, title_line.strip(), month, day, page_no))
                elif debug_headers:
                    print(f"[HDR_DROP] {volume_id} p{page_no} | C | {month} {day} / {title_line.strip()} | evidence=NO")
                i = j + 1
                continue

        # E) Title, m/d
        m4 = HDR_TITLE_NUMDATE.match(text)
        if m4:
            title = m4.group("title").strip()
            mnum = int(m4.group("m"))
            day = int(m4.group("day"))
            ok = (1 <= mnum <= 12 and 1 <= day <= 31 and start_evidence_after(i))
            if ok:
                headers.append((i, title, f"__NUM__{mnum}", day, page_no))
            elif debug_headers:
                print(f"[HDR_DROP] {volume_id} p{page_no} | E | {title} , {mnum}/{day} | evidence=NO")
            i += 1
            continue

        # F) Title, MonAbbr Day
        m5 = HDR_TITLE_MONABBR.match(text)
        if m5:
            title = m5.group("title").strip()
            mon = (m5.group("mon") or "").lower()
            day = int(m5.group("day"))
            mnum = monabbr_to_num.get(mon)
            ok = (mnum and 1 <= day <= 31 and start_evidence_after(i))
            if ok:
                headers.append((i, title, f"__NUM__{mnum}", day, page_no))
            elif debug_headers:
                print(f"[HDR_DROP] {volume_id} p{page_no} | F | {title} , {mon} {day} | evidence=NO")
            i += 1
            continue

        i += 1

    if not headers:
        raise RuntimeError(f"No devotion headers detected in PDF: {path.name}")

    headers.sort(key=lambda x: x[0])

    # -------------------------------------------------
    # ✅ 핵심: 반복 헤더(같은 날짜)가 연속으로 나오면 제거
    #    (다음 페이지 상단에 같은 날짜 헤더가 반복되는 "continuation" 케이스)
    # -------------------------------------------------
    def monthtoken_to_mm(month_token: str) -> Optional[int]:
        if month_token.startswith("__NUM__"):
            try:
                return int(month_token.replace("__NUM__", ""))
            except Exception:
                return None
        return month_to_num.get(month_token.capitalize())

    condensed: list[tuple[int, str, str, int, int, str]] = []  # (start_i,title,month_token,day,start_page,iso)
    last_iso: Optional[str] = None

    for (start_i, title, month_token, day, start_page) in headers:
        mm = monthtoken_to_mm(month_token)
        if not mm:
            continue

        # ✅ 최종 달력 유효성 검증
        try:
            datetime.date(year, mm, day)
        except ValueError:
            if debug_headers:
                print(f"[SKIP] invalid date: {year}-{mm:02d}-{day:02d} | {title} | {path.name}:p{start_page}")
            continue

        iso_date = f"{year:04d}-{mm:02d}-{day:02d}"

        if last_iso == iso_date:
            if debug_headers:
                print(f"[DEDUP] repeated header for {iso_date} at p{start_page} (ignored)")
            continue

        condensed.append((start_i, title, month_token, day, start_page, iso_date))
        last_iso = iso_date

    if not condensed:
        raise RuntimeError(f"No valid devotion headers after filtering in PDF: {path.name}")

    # -------------------------------------------------
    # body 생성
    # -------------------------------------------------
    def is_header_like_line(t: str) -> bool:
        if HDR_TITLE_DATE.match(t): return True
        if HDR_TITLE_DATE_NOCOMMA.match(t) and not DOT_LEADER_RE.search(t): return True
        if HDR_TITLE_MONTH_ONLY.match(t): return True
        if HDR_DATE_ONLY.match(t): return True
        if HDR_TITLE_NUMDATE.match(t): return True
        if HDR_TITLE_MONABBR.match(t): return True
        return False

    for k, (start_i, title, month_token, day, start_page, iso_date) in enumerate(condensed):
        body_start = next_nonempty_idx(start_i + 1)
        end_i = condensed[k + 1][0] if k + 1 < len(condensed) else len(lines)

        body_parts: list[str] = []
        for j in range(body_start, end_i):
            t, _ = lines[j]
            if not t:
                continue
            if NUM_ONLY_RE.match(t):
                continue
            if BRACKET_PNUM_RE.match(t):
                continue
            if DOT_LEADER_RE.search(t):
                continue
            if is_header_like_line(t):
                continue

            body_parts.append(t)

        body = "\n".join(body_parts).strip()

        yield DevotionRecord(
            volume_id=volume_id,
            lang=lang,
            d=iso_date,
            title=title,
            body=body,
            source_ref=f"pdf:{path.name}:p{start_page}",
        )


# -----------------------------
# DB schema / operations
# -----------------------------

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS build_meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS volumes (
  volume_id TEXT PRIMARY KEY,
  lang TEXT NOT NULL,
  title TEXT NOT NULL,
  year INTEGER NOT NULL,
  source_type TEXT NOT NULL,
  source_path TEXT NOT NULL,
  source_sha256 TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS devotions (
  devotion_pk INTEGER PRIMARY KEY AUTOINCREMENT,
  volume_id TEXT NOT NULL REFERENCES volumes(volume_id) ON DELETE CASCADE,
  lang TEXT NOT NULL,
  d TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  source_ref TEXT NOT NULL,
  UNIQUE(volume_id, d)
);

CREATE TABLE IF NOT EXISTS segments (
  segment_pk INTEGER PRIMARY KEY AUTOINCREMENT,
  devotion_pk INTEGER NOT NULL REFERENCES devotions(devotion_pk) ON DELETE CASCADE,
  ord INTEGER NOT NULL,
  content TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS segments_fts USING fts5(
  content,
  segment_pk UNINDEXED,
  devotion_pk UNINDEXED
);

CREATE TABLE IF NOT EXISTS citations (
  citation_pk INTEGER PRIMARY KEY AUTOINCREMENT,
  segment_pk INTEGER NOT NULL REFERENCES segments(segment_pk) ON DELETE CASCADE,
  raw TEXT NOT NULL,
  norm_key TEXT NOT NULL,
  source_lang TEXT NOT NULL,
  confidence REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS citation_index (
  norm_key TEXT NOT NULL,
  segment_pk INTEGER NOT NULL REFERENCES segments(segment_pk) ON DELETE CASCADE,
  devotion_pk INTEGER NOT NULL REFERENCES devotions(devotion_pk) ON DELETE CASCADE,
  volume_id TEXT NOT NULL,
  d TEXT NOT NULL,
  PRIMARY KEY (norm_key, segment_pk)
);

CREATE INDEX IF NOT EXISTS idx_devotions_date ON devotions(d);
CREATE INDEX IF NOT EXISTS idx_segments_devotion ON segments(devotion_pk);
CREATE INDEX IF NOT EXISTS idx_citations_normkey ON citations(norm_key);
CREATE INDEX IF NOT EXISTS idx_citation_index_normkey ON citation_index(norm_key);
"""

def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()

def drop_all(conn: sqlite3.Connection) -> None:
    # FOREIGN KEY ON 상태에서 DROP 순서가 꼬이면 IntegrityError가 날 수 있어서
    # 드랍할 때만 OFF로 내리고 다시 ON.
    conn.execute("PRAGMA foreign_keys=OFF;")
    for tbl in ["citation_index", "citations", "segments_fts", "segments", "devotions", "volumes", "build_meta"]:
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.commit()

def upsert_volume(conn: sqlite3.Connection, spec: VolumeSpec) -> None:
    sha = sha256_file(spec.source_path)
    conn.execute(
        """
        INSERT INTO volumes(volume_id, lang, title, year, source_type, source_path, source_sha256)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(volume_id) DO UPDATE SET
          lang=excluded.lang,
          title=excluded.title,
          year=excluded.year,
          source_type=excluded.source_type,
          source_path=excluded.source_path,
          source_sha256=excluded.source_sha256
        """,
        (spec.volume_id, spec.lang, spec.title, spec.year, spec.source_type, str(spec.source_path), sha)
    )

def volume_changed(conn: sqlite3.Connection, spec: VolumeSpec) -> bool:
    row = conn.execute("SELECT source_sha256 FROM volumes WHERE volume_id=?", (spec.volume_id,)).fetchone()
    if not row:
        return True
    return row["source_sha256"] != sha256_file(spec.source_path)

def delete_volume_data(conn: sqlite3.Connection, volume_id: str) -> None:
    conn.execute("DELETE FROM devotions WHERE volume_id=?", (volume_id,))

def split_into_segments(body: str) -> list[str]:
    parts = [p.strip() for p in re.split(r"\n{2,}|\r\n\r\n", body) if p.strip()]
    out: list[str] = []
    for p in parts:
        if len(p) > 1400 and "\n" in p:
            out.extend([x.strip() for x in p.splitlines() if x.strip()])
        else:
            out.append(p)
    return out

def insert_devotion(conn: sqlite3.Connection, rec: DevotionRecord) -> int:
    conn.execute(
        """
        INSERT OR REPLACE INTO devotions(volume_id, lang, d, title, body, source_ref)
        VALUES(?,?,?,?,?,?)
        """,
        (rec.volume_id, rec.lang, rec.d, rec.title, rec.body, rec.source_ref)
    )
    row = conn.execute(
        "SELECT devotion_pk FROM devotions WHERE volume_id=? AND d=?",
        (rec.volume_id, rec.d)
    ).fetchone()
    if not row:
        raise RuntimeError("Failed to read back devotion_pk")
    return int(row["devotion_pk"])

def insert_segments_and_fts(conn: sqlite3.Connection, devotion_pk: int, seg_texts: list[str]) -> list[int]:
    seg_pks: list[int] = []
    for i, content in enumerate(seg_texts):
        cur = conn.execute(
            "INSERT INTO segments(devotion_pk, ord, content) VALUES(?,?,?)",
            (devotion_pk, i, content)
        )
        segment_pk = int(cur.lastrowid)
        seg_pks.append(segment_pk)
        conn.execute(
            "INSERT INTO segments_fts(content, segment_pk, devotion_pk) VALUES(?,?,?)",
            (content, segment_pk, devotion_pk)
        )
    return seg_pks

def insert_citations_and_index(
    conn: sqlite3.Connection,
    detector: CitationDetector,
    devotion_pk: int,
    volume_id: str,
    d: str,
    seg_pks: list[int],
    seg_texts: list[str],
) -> None:
    for segment_pk, text in zip(seg_pks, seg_texts):
        for h in detector.detect_in_text(text, segment_pk):
            conn.execute(
                "INSERT INTO citations(segment_pk, raw, norm_key, source_lang, confidence) VALUES(?,?,?,?,?)",
                (h.segment_pk, h.raw, h.norm_key, h.source_lang, h.confidence)
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO citation_index(norm_key, segment_pk, devotion_pk, volume_id, d)
                VALUES(?,?,?,?,?)
                """,
                (h.norm_key, h.segment_pk, devotion_pk, volume_id, d)
            )


# -----------------------------
# Best-per-date scoring
# -----------------------------

_SCRIPTURE_SCORE_PAT = re.compile(
    r"\b\d?\s*[A-Za-z][A-Za-z’']+(?:\s+[A-Za-z][A-Za-z’']+){0,3}\s+\d+:\d+(?:\s*[-–]\s*\d+)?(?:\s*,\s*\d+)?\b",
    re.IGNORECASE
)

def _score_devotion(rec: DevotionRecord) -> int:
    body = rec.body or ""
    lines = [x.strip() for x in body.splitlines() if x.strip()]

    score = 0
    score += min(len(body), 9000) // 18

    for t in lines[:6]:
        if _SCRIPTURE_SCORE_PAT.search(t):
            score += 250
            break

    if len(body) < 450:
        score -= 400

    if len((rec.title or "").strip()) < 6:
        score -= 150

    return score


# -----------------------------
# Volume discovery
# -----------------------------

def discover_volumes(source_dir: Path) -> list[VolumeSpec]:
    specs: list[VolumeSpec] = []

    en_dir = source_dir / "EN"
    ko_dir = source_dir / "KO"

    if en_dir.exists():
        for p in sorted(en_dir.glob("*")):
            if not p.is_file():
                continue
            ext = p.suffix.lower()
            if ext not in [".pdf", ".docx"]:
                continue
            year = guess_year_from_filename(p)
            if year is None:
                continue
            stype = "pdf" if ext == ".pdf" else "docx"
            specs.append(VolumeSpec(
                volume_id=f"EN_{year}",
                lang="en",
                title=f"{safe_title_from_filename(p)} (EN)",
                year=year,
                source_type=stype,
                source_path=p
            ))

    if ko_dir.exists():
        for p in sorted(ko_dir.glob("*")):
            if not p.is_file():
                continue
            if p.suffix.lower() != ".docx":
                continue
            year = guess_year_from_filename(p)
            if year is None:
                continue
            specs.append(VolumeSpec(
                volume_id=f"KO_{year}",
                lang="ko",
                title=f"{safe_title_from_filename(p)} (KO)",
                year=year,
                source_type="docx",
                source_path=p
            ))

    uniq: dict[str, VolumeSpec] = {}
    for s in specs:
        uniq[s.volume_id] = s
    return [uniq[k] for k in sorted(uniq.keys())]


# -----------------------------
# Build pipeline
# -----------------------------

def build_volume(conn: sqlite3.Connection, spec: VolumeSpec, detector: CitationDetector, debug_headers: bool) -> None:
    delete_volume_data(conn, spec.volume_id)

    if spec.source_type == "docx":
        it = parse_docx_to_devotions(spec.source_path, spec.volume_id, spec.lang)
    elif spec.source_type == "pdf":
        it = parse_pdf_to_devotions(spec.source_path, spec.volume_id, spec.lang, debug_headers=debug_headers)
    else:
        raise ValueError(f"Unknown source_type: {spec.source_type}")

    best_by_date: dict[str, tuple[int, DevotionRecord]] = {}
    for rec in it:
        sc = _score_devotion(rec)
        prev = best_by_date.get(rec.d)
        if prev is None or sc > prev[0]:
            best_by_date[rec.d] = (sc, rec)

    cnt = 0
    for d in sorted(best_by_date.keys()):
        rec = best_by_date[d][1]
        devotion_pk = insert_devotion(conn, rec)
        seg_texts = split_into_segments(rec.body)
        seg_pks = insert_segments_and_fts(conn, devotion_pk, seg_texts)
        insert_citations_and_index(
            conn=conn,
            detector=detector,
            devotion_pk=devotion_pk,
            volume_id=rec.volume_id,
            d=rec.d,
            seg_pks=seg_pks,
            seg_texts=seg_texts
        )
        cnt += 1

    print(f"[OK] Built {spec.volume_id} ({spec.source_type}) -> {cnt} devotions")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH))
    ap.add_argument("--source_dir", type=str, default=str(DEFAULT_SOURCE_DIR))
    ap.add_argument("--abbr_map", type=str, default=str(DEFAULT_ABBR_MAP))
    ap.add_argument("--rebuild", action="store_true", help="Drop + recreate all tables")
    ap.add_argument("--incremental", action="store_true", help="Only rebuild changed volumes")
    ap.add_argument("--debug_headers", action="store_true", help="Print header-drop diagnostics (PDF only)")
    args = ap.parse_args()

    db_path = Path(args.db)
    source_dir = Path(args.source_dir)
    abbr_map_path = Path(args.abbr_map)

    if not source_dir.exists():
        print(f"[ERR] SOURCE dir not found: {source_dir}", file=sys.stderr)
        return 2

    abbr_map = load_abbr_map(abbr_map_path)
    detector = CitationDetector(abbr_map)

    specs = discover_volumes(source_dir)
    if not specs:
        print("[ERR] No volumes discovered. Ensure filenames contain year like 2013..2025.", file=sys.stderr)
        return 3

    conn = connect_db(db_path)
    if args.rebuild:
        drop_all(conn)
    init_db(conn)

    # incremental 모드에서는 upsert 전에 변경 여부를 판정해야 함.
    changed_map: dict[str, bool] = {}
    for spec in specs:
        changed_map[spec.volume_id] = volume_changed(conn, spec) if args.incremental else True
        upsert_volume(conn, spec)
    conn.commit()

    for spec in specs:
        if args.incremental and not changed_map.get(spec.volume_id, True):
            print(f"[SKIP] {spec.volume_id} (no change)")
            continue

        print(f"[BUILD] {spec.volume_id} | {spec.source_path.name}")
        try:
            build_volume(conn, spec, detector, debug_headers=args.debug_headers)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[FAIL] {spec.volume_id} | {spec.source_path.name} -> {e}", file=sys.stderr)
            continue

    print(f"[DONE] DB: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
