# fix_volume_paths.py
import sqlite3
from pathlib import Path

DB_PATH = Path("DATA/egw_devotionals.sqlite")
ROOT = Path(__file__).resolve().parent
SRC_EN = ROOT / "SOURCE" / "EN"
SRC_KO = ROOT / "SOURCE" / "KO"

def find_best_file(lang: str, year: int, ext: str) -> Path | None:
    base = SRC_EN if lang.lower() == "en" else SRC_KO
    if not base.exists():
        return None

    # 1) 가장 흔한 패턴: "2013 ..."로 시작
    cands = list(base.glob(f"{year} *.{ext}"))
    if cands:
        return cands[0]

    # 2) 혹시 다른 이름이면 year 포함으로 넓게
    cands = list(base.glob(f"*{year}*.{ext}"))
    if cands:
        return cands[0]

    # 3) 마지막: 확장자 전체 중 하나
    cands = list(base.glob(f"*.{ext}"))
    return cands[0] if cands else None

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"[ERR] DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("select volume_id, lang, year, source_type, source_path from volumes order by volume_id").fetchall()

    fixed = 0
    for r in rows:
        vid = r["volume_id"]
        lang = r["lang"]
        year = int(r["year"])
        stype = r["source_type"]
        spath = r["source_path"] or ""

        # 확장자 결정
        if stype == "pdf":
            ext = "pdf"
        elif stype == "docx":
            ext = "docx"
        else:
            continue

        # 현재 path가 정상인지 체크
        p = Path(spath)
        ok = p.exists()

        if not ok:
            best = find_best_file(lang, year, ext)
            if not best:
                print(f"[MISS] {vid}: cannot find a matching file under SOURCE/{'EN' if lang=='en' else 'KO'}")
                continue

            # ✅ 이식성 위해 상대경로로 저장
            rel = best.relative_to(ROOT)
            conn.execute("update volumes set source_path=? where volume_id=?", (str(rel), vid))
            fixed += 1
            print(f"[FIX] {vid}: {spath!r}  ->  {str(rel)!r}")

    conn.commit()
    conn.close()
    print(f"[DONE] fixed={fixed}")

if __name__ == '__main__':
    main()
