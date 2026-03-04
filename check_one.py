#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from pathlib import Path

DB_PATH = Path("DATA/egw_devotionals.sqlite")

def q1(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()

def qall(conn, sql, params=()):
    return conn.execute(sql, params).fetchall()

def main():
    print(f"[DB_PATH] {DB_PATH} (exists={DB_PATH.exists()})")
    if DB_PATH.exists():
        try:
            size = DB_PATH.stat().st_size
            print(f"[DB_SIZE] {size} bytes")
        except Exception as e:
            print(f"[DB_SIZE] (error) {e}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # 0) 테이블 존재 확인
    tables = qall(conn, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    print("\n[TABLES]")
    for r in tables:
        print(" -", r["name"])

    # devotions 테이블이 없으면 여기서 끝
    has_devotions = q1(conn, "SELECT 1 FROM sqlite_master WHERE type='table' AND name='devotions'")
    if not has_devotions:
        print("\n[ERR] 'devotions' table not found. build_db.py --rebuild 를 먼저 실행해야 합니다.")
        return

    # 1) 전체 row 수 / 날짜 범위
    total = q1(conn, "SELECT COUNT(*) AS c FROM devotions")["c"]
    print(f"\n[DEVOTIONS] total_rows = {total}")

    if total == 0:
        print("[ERR] devotions 테이블이 비어있습니다. build_db.py --rebuild 를 실행했는지 확인하세요.")
        return

    minmax = q1(conn, "SELECT MIN(d) AS mind, MAX(d) AS maxd FROM devotions")
    print(f"[DATE RANGE] {minmax['mind']} ~ {minmax['maxd']}")

    # 2) volume_id 목록 (상위 30개만)
    vols = qall(conn, "SELECT volume_id, COUNT(*) AS c FROM devotions GROUP BY volume_id ORDER BY volume_id")
    print(f"\n[VOLUMES] distinct = {len(vols)}")
    for r in vols[:30]:
        print(f" - {r['volume_id']}: {r['c']}")
    if len(vols) > 30:
        print(f" ... (showing 30 / {len(vols)})")

    # 3) 사용자가 넣은 테스트 날짜들이 실제로 존재하는지: (volume 무시하고) 검색
    tests = ["2016-02-18", "2016-02-29", "2020-02-28", "2024-02-28"]
    print("\n[DATE EXISTS?] (ignoring volume_id)")
    for d in tests:
        r = q1(conn,
               "SELECT volume_id, title, LENGTH(body) AS blen FROM devotions WHERE d=? LIMIT 1",
               (d,))
        if r:
            print(f" - {d}: FOUND in {r['volume_id']} | body_len={r['blen']} | title={r['title']}")
        else:
            print(f" - {d}: NOT FOUND")

    # 4) 사용자가 기대하는 volume_id가 실제로 있는지 확인 + 대소문자/공백 문제 진단
    expected_vols = ["EN_2016", "EN_2020", "EN_2024"]
    print("\n[VOLUME EXISTS?]")
    for v in expected_vols:
        r = q1(conn, "SELECT COUNT(*) AS c FROM devotions WHERE volume_id=?", (v,))
        if r and r["c"] > 0:
            print(f" - {v}: FOUND rows={r['c']}")
        else:
            # 유사 검색 (대소문자 무시)
            sim = qall(conn,
                       "SELECT volume_id, COUNT(*) AS c FROM devotions "
                       "WHERE LOWER(volume_id)=LOWER(?) GROUP BY volume_id",
                       (v,))
            if sim:
                for s in sim:
                    print(f" - {v}: NOT EXACT, but found '{s['volume_id']}' rows={s['c']}")
            else:
                print(f" - {v}: NOT FOUND")

    # 5) 샘플 5개 출력 (DB가 제대로 들어왔는지 감 잡기)
    print("\n[SAMPLES] first 5 rows:")
    rows = qall(conn, "SELECT volume_id, d, title, LENGTH(body) AS blen FROM devotions ORDER BY volume_id, d LIMIT 5")
    for r in rows:
        print(f" - {r['volume_id']} | {r['d']} | body_len={r['blen']} | {r['title']}")

    conn.close()

if __name__ == "__main__":
    main()
