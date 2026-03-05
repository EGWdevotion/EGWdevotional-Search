
from io import BytesIO
import pandas as pd
import streamlit as st
import sqlite3
import re
from pathlib import Path
from html import escape
import calendar
from collections import defaultdict

# --------------------------------------------------
# matplotlib optional (없어도 앱은 돌아가되, 그래프만 비활성)
# --------------------------------------------------
try:
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm  # noqa: F401

    plt.rcParams["font.family"] = "Malgun Gothic"
    plt.rcParams["axes.unicode_minus"] = False
    HAS_MPL = True
except Exception:
    HAS_MPL = False

# --------------------------------------------------
# DB
# --------------------------------------------------
DB_PATH = Path("data/egw_devotionals.sqlite")

st.set_page_config(
    page_title="EGW 기도력 검색",
    page_icon="📖",
    layout="wide"
)
st.title("📖 EGW 기도력 검색 (영문/한글)")

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# --------------------------------------------------
# 📚 약자 → 풀네임 매핑 (전체)
# --------------------------------------------------
BOOK_MAP = {
    "1설교": "설교와 강연 1권",
    "2설교": "설교와 강연 2권",
    "1기별": "가려 뽑은 기별 1권",
    "2기별": "가려 뽑은 기별 2권",
    "3기별": "가려 뽑은 기별 3권",
    "1보감": "증언보감 1권",
    "2보감": "증언보감 2권",
    "3보감": "증언보감 3권",
    "1증언": "교회증언 1권",
    "2증언": "교회증언 2권",
    "3증언": "교회증언 3권",
    "4증언": "교회증언 4권",
    "5증언": "교회증언 5권",
    "6증언": "교회증언 6권",
    "7증언": "교회증언 7권",
    "8증언": "교회증언 8권",
    "9증언": "교회증언 9권",
    "가건": "가정과 건강",
    "가정": "재림신도의 가정",
    "교권": "교회에 보내는 권면",
    "교육": "교육",
    "구호": "구호봉사",
    "기도": "기도",
    "높은 부르심": "우리의 높은 부르심",
    "대결": "대결",
    "도시": "도시 선교",
    "딸들": "하나님의 딸들",
    "동행": "동행",
    "리더십": "그리스도인 리더십",
    "1마음": "그리스도인의 마음과 품성과 인격 1권",
    "2마음": "그리스도인의 마음과 품성과 인격 2권",
    "목사": "목사와 복음 교역자에게 보내는 증언",
    "목회": "목회봉사",
    "문선": "그리스도인 문서 선교",
    "문전": "문서전도봉사",
    "믿음": "믿음과 행함",
    "바울": "바울의 생애",
    "보훈": "산상보훈",
    "복음": "복음 교역자",
    "부모": "부모와 교사와 학생에게 보내는 권면",
    "부조": "부조와 선지자",
    "사건": "마지막 날 사건들",
    "살아": "살아남는 이들",
    "생애": "생애의 빛",
    "선교": "그리스도인 선교봉사",
    "선지": "선지자와 왕",
    "성소": "성소에 계신 그리스도",
    "성화": "성화된 생애",
    "소망": "시대의 소망",
    "실물": "실물교훈",
    "안교": "안식일학교 사업에 관한 권면",
    "음식": "식생활과 음식물에 관한 권면",
    "의료": "의료봉사",
    "인류": "인류의 빛",
    "자녀": "새 자녀 지도법",
    "자서": "엘렌 G. 화잇 자서전",
    "쟁투": "각 시대의 대쟁투",
    "전도": "복음전도",
    "절제": "절제생활",
    "정로": "정로의 계단",
    "청년": "청년에게 보내는 기별",
    "청지기": "청지기에게 보내는 권면",
    "초기": "초기문집",
    "치료": "치료봉사",
    "하늘": "하늘",
    "행실": "성적 행실과 간음과 이혼에 관한 증언",
    "행적": "사도행적",
    "화잇주석": "엘렌 G. 화잇의 주석",
}
ALL_BOOKS = sorted(BOOK_MAP.keys())


def build_where_lang(search_lang: str) -> str:
    if search_lang == "EN":
        return "volume_id LIKE 'EN_%'"
    if search_lang == "KO":
        return "volume_id LIKE 'KO_%'"
    return "(volume_id LIKE 'EN_%' OR volume_id LIKE 'KO_%')"


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def highlight_phrase_only(text: str, phrase: str) -> str:
    if not text:
        return ""
    raw_html = escape(text).replace("\n", "<br>")
    p = (phrase or "").strip()
    if not p:
        return raw_html
    try:
        rx = re.compile(re.escape(p), re.IGNORECASE)
        return rx.sub(r"<mark>\g<0></mark>", raw_html)
    except re.error:
        return raw_html


def highlight_words(text: str, query: str) -> str:
    if not text:
        return ""
    raw_html = escape(text).replace("\n", "<br>")
    q = (query or "").strip()
    if not q:
        return raw_html

    parts = [p for p in re.split(r"\s+", q) if p.strip()]
    parts = [p.strip("()[]{}<>.,;:\"'“”‘’") for p in parts]
    parts = [p for p in parts if p]
    if not parts:
        return raw_html

    pattern = "(" + "|".join(re.escape(p) for p in sorted(set(parts), key=len, reverse=True)) + ")"
    try:
        rx = re.compile(pattern, re.IGNORECASE)
        return rx.sub(r"<mark>\1</mark>", raw_html)
    except re.error:
        return raw_html


def fetch_pair_by_date(d: str):
    en = conn.execute(
        """
        SELECT d, title, body, volume_id
        FROM devotions
        WHERE d=? AND volume_id LIKE 'EN_%'
        ORDER BY volume_id
        LIMIT 1
        """,
        (d,),
    ).fetchone()

    ko = conn.execute(
        """
        SELECT d, title, body, volume_id
        FROM devotions
        WHERE d=? AND volume_id LIKE 'KO_%'
        ORDER BY volume_id
        LIMIT 1
        """,
        (d,),
    ).fetchone()

    return en, ko


# --------------------------------------------------
# Keyword Search
# --------------------------------------------------
def tokenize_simple(q: str) -> list[str]:
    q = (q or "").strip()
    if not q:
        return []
    toks = []
    for t in re.split(r"\s+", q):
        t = t.strip("()[]{}<>.,;:\"'“”‘’")
        if t:
            toks.append(t)
    return toks[:12]


def keyword_search_dates_phrase(query: str, search_lang: str, limit_dates: int) -> list[str]:
    where_lang = build_where_lang(search_lang)
    q = (query or "").strip()
    if not q:
        return []
    sql = f"""
    SELECT d
    FROM devotions
    WHERE body LIKE ?
      AND {where_lang}
    GROUP BY d
    ORDER BY d DESC
    LIMIT ?
    """
    rows = conn.execute(sql, (f"%{q}%", limit_dates)).fetchall()
    return [r["d"] for r in rows]


def keyword_search_dates_tokens_and(query: str, search_lang: str, limit_dates: int) -> list[str]:
    where_lang = build_where_lang(search_lang)
    tokens = tokenize_simple(query)
    if not tokens:
        return []

    conds = []
    params = []
    for t in tokens:
        conds.append("body LIKE ?")
        params.append(f"%{t}%")

    sql = f"""
    SELECT d
    FROM devotions
    WHERE {" AND ".join(conds)}
      AND {where_lang}
    GROUP BY d
    ORDER BY d DESC
    LIMIT ?
    """
    params.append(limit_dates)
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [r["d"] for r in rows]


# --------------------------------------------------
# Citation Search
# --------------------------------------------------
def page_in_pagespec_exact(page: int, pagespec: str) -> bool:
    if not pagespec:
        return False
    s = pagespec.replace("–", "-").replace("—", "-").replace("~", "-")
    s = re.sub(r"\s+", " ", s)

    for a, b in re.findall(r"(\d{1,4})\s*-\s*(\d{1,4})", s):
        lo = int(a)
        hi = int(b)
        if lo <= page <= hi or hi <= page <= lo:
            return True

    page_pat = re.compile(r"(?<!\d)" + re.escape(str(page)) + r"(?!\d)")
    return bool(page_pat.search(s))


def body_has_citation(body: str, short: str, full: str, page: int) -> bool:
    if not body:
        return False

    paren_rx = re.compile(r"[（(]\s*" + re.escape(short) + r"\s*[,，]\s*([^）)]+)[）)]")
    for m in paren_rx.finditer(body):
        if page_in_pagespec_exact(page, m.group(1)):
            return True

    bracket_rx = re.compile(r"『\s*" + re.escape(full) + r"\s*』\s*[,，]?\s*([0-9][0-9\s,\-~–—]+)")
    for m in bracket_rx.finditer(body):
        if page_in_pagespec_exact(page, m.group(1)):
            return True

    return False


def citation_search_dates(short: str, page: int, search_lang: str, limit_dates: int = 50) -> list[str]:
    where_lang = build_where_lang(search_lang)
    full = BOOK_MAP.get(short, short)
    page_str = str(page)

    sql = f"""
    SELECT d, body
    FROM devotions
    WHERE {where_lang}
      AND (body LIKE ? OR body LIKE ?)
      AND body LIKE ?
    ORDER BY d DESC
    LIMIT 3000
    """
    rows = conn.execute(sql, (f"%{short}%", f"%{full}%", f"%{page_str}%")).fetchall()

    dates = []
    seen = set()
    for r in rows:
        d = r["d"]
        if d in seen:
            continue
        if body_has_citation(r["body"], short=short, full=full, page=page):
            seen.add(d)
            dates.append(d)
        if len(dates) >= limit_dates:
            break
    return dates


def highlight_citation_only(body: str, short: str, full: str) -> str:
    if not body:
        return ""
    html = escape(body).replace("\n", "<br>")

    paren_rx = re.compile(r"([（(]\s*" + re.escape(short) + r"\s*[,，]\s*[^）)]+[）)])")
    html = paren_rx.sub(r"<mark>\1</mark>", html)

    bracket_rx = re.compile(r"(『\s*" + re.escape(full) + r"\s*』\s*[,，]?\s*[0-9][0-9\s,\-~–—]+)")
    html = bracket_rx.sub(r"<mark>\1</mark>", html)

    return html


# --------------------------------------------------
# Book Stats (KO 기준)
# --------------------------------------------------
def _compile_book_regex():
    compiled = {}
    for short, full in BOOK_MAP.items():
        paren = re.compile(r"[（(]\s*" + re.escape(short) + r"\s*[,，]\s*[^）)]+[）)]")
        bracket = re.compile(r"『\s*" + re.escape(full) + r"\s*』\s*[,，]?\s*[0-9][0-9\s,\-~–—]+")
        compiled[short] = (paren, bracket)
    return compiled


BOOK_RX = _compile_book_regex()


@st.cache_data(show_spinner=False)
def stats_count_all_citations_ko():
    counts = defaultdict(int)
    rows = conn.execute(
        """
        SELECT body
        FROM devotions
        WHERE volume_id LIKE 'KO_%'
        """
    ).fetchall()

    for r in rows:
        body = r["body"] or ""
        if not body:
            continue
        for short, (rx1, rx2) in BOOK_RX.items():
            counts[short] += len(rx1.findall(body))
            counts[short] += len(rx2.findall(body))
    return dict(counts)


@st.cache_data(show_spinner=False)
def stats_count_book_by_year_ko(selected_short: str):
    year_counts = defaultdict(int)
    rx1, rx2 = BOOK_RX[selected_short]

    rows = conn.execute(
        """
        SELECT d, body
        FROM devotions
        WHERE volume_id LIKE 'KO_%'
        """
    ).fetchall()

    for r in rows:
        body = r["body"] or ""
        if not body:
            continue
        year = (r["d"] or "")[:4]
        if not year:
            continue
        year_counts[year] += len(rx1.findall(body))
        year_counts[year] += len(rx2.findall(body))
    return dict(year_counts)


# --------------------------------------------------
# Page Explorer helpers
# --------------------------------------------------
def split_paragraphs(text: str) -> list[str]:
    parts = re.split(r"\n\s*\n+", text or "")
    return [p.strip() for p in parts if p.strip()]


def parse_pagespec_to_pages(pagespec: str, max_expand: int = 200) -> list[int]:
    if not pagespec:
        return []
    s = pagespec.replace("–", "-").replace("—", "-").replace("~", "-")
    s = s.replace("，", ",")
    s = re.sub(r"\s+", " ", s).strip()

    pages = []
    chunks = [c.strip() for c in s.split(",") if c.strip()]
    for c in chunks:
        m = re.match(r"^(\d{1,4})\s*-\s*(\d{1,4})$", c)
        if m:
            a = int(m.group(1))
            b = int(m.group(2))
            lo, hi = (a, b) if a <= b else (b, a)
            length = hi - lo + 1
            if length <= max_expand:
                pages.extend(list(range(lo, hi + 1)))
            else:
                pages.append(lo)
                pages.append(hi)
        else:
            m2 = re.match(r"^(\d{1,4})$", c)
            if m2:
                pages.append(int(m2.group(1)))

    return pages


def extract_pages_for_book(body: str, short: str, full: str) -> list[int]:
    pages = []

    paren_rx = re.compile(r"[（(]\s*" + re.escape(short) + r"\s*[,，]\s*([^）)]+)[）)]")
    for m in paren_rx.finditer(body or ""):
        pages.extend(parse_pagespec_to_pages(m.group(1)))

    bracket_rx = re.compile(r"『\s*" + re.escape(full) + r"\s*』\s*[,，]?\s*([0-9][0-9\s,\-~–—]+)")
    for m in bracket_rx.finditer(body or ""):
        pages.extend(parse_pagespec_to_pages(m.group(1)))

    return pages


@st.cache_data(show_spinner=False)
def page_distribution_ko(short: str):
    full = BOOK_MAP.get(short, short)
    counts = defaultdict(int)

    rows = conn.execute(
        """
        SELECT body
        FROM devotions
        WHERE volume_id LIKE 'KO_%'
        """
    ).fetchall()

    for r in rows:
        body = r["body"] or ""
        if not body:
            continue
        pages = extract_pages_for_book(body, short, full)
        for p in pages:
            counts[p] += 1

    return dict(counts)


# --------------------------------------------------
# UI
# --------------------------------------------------



mode = st.sidebar.radio(
    "메뉴",
    ["날짜 검색", "키워드 검색", "출처 검색", "책별 통계", "페이지 탐색기"],
    key="mode_radio"
)



st.sidebar.markdown("---")
st.sidebar.markdown(
    "<small>📖 EGW 기도력 검색 v1.0<br>제작: 김효준<br>© 2026</small>",
    unsafe_allow_html=True
)
# --------------------------------------------------
# 날짜 검색
# --------------------------------------------------


# --------------------------------------------------
# 키워드 검색
# --------------------------------------------------
elif mode == "키워드 검색":
    query = st.text_area("키워드 또는 문장 입력 (EN 또는 KO)", height=120, key="kw_query")

    search_lang_label = st.sidebar.selectbox(
        "검색 범위",
        ["영문(EN)", "한글(KO)", "통합(BOTH)"],
        index=2,
        key="kw_lang_label"
    )
    search_lang = {"영문(EN)": "EN", "한글(KO)": "KO", "통합(BOTH)": "BOTH"}[search_lang_label]

    limit_dates = st.sidebar.slider("최대 결과 날짜 수", 10, 300, 50, 10, key="kw_limit")
    phrase_mode = st.checkbox("문구 그대로만 찾기(정확 검색)", value=True, key="kw_phrase")
    highlight_on = st.sidebar.checkbox("강조 표시(하이라이트)", value=True, key="kw_hl")

    if query.strip():
        dates = keyword_search_dates_phrase(query, search_lang, limit_dates) if phrase_mode else keyword_search_dates_tokens_and(query, search_lang, limit_dates)
        st.write(f"결과: {len(dates)}개 날짜")

        download_rows = []

        for d in dates:
            en, ko = fetch_pair_by_date(d)

            download_rows.append({
                "date": d,
                "EN_title": en["title"] if en else "",
                "EN_body": en["body"] if en else "",
                "KO_title": ko["title"] if ko else "",
                "KO_body": ko["body"] if ko else "",
            })

            if en:
                title_line = f"{d} | {en['title']} (EN:{en['volume_id']})"
                if ko:
                    title_line += f" ⇄ (KO:{ko['volume_id']})"
            elif ko:
                title_line = f"{d} | {ko['title']} (KO:{ko['volume_id']})"
            else:
                title_line = f"{d} | (no rows?)"

            with st.expander(title_line, expanded=False):
                left, right = st.columns(2)
                with left:
                    if en:
                        body_html = highlight_phrase_only(en["body"], query) if (highlight_on and phrase_mode) else (
                            highlight_words(en["body"], query) if highlight_on else escape(en["body"]).replace("\n", "<br>")
                        )
                        st.markdown(body_html, unsafe_allow_html=True)
                    else:
                        st.info("EN: (없음)")
                with right:
                    if ko:
                        body_html = highlight_phrase_only(ko["body"], query) if (highlight_on and phrase_mode) else (
                            highlight_words(ko["body"], query) if highlight_on else escape(ko["body"]).replace("\n", "<br>")
                        )
                        st.markdown(body_html, unsafe_allow_html=True)
                    else:
                        st.info("KO: (없음)")

        # 다운로드 버튼 (for 밖, if query 안)
        if download_rows:
            df = pd.DataFrame(download_rows)

            st.download_button(
                "⬇ CSV 다운로드",
                df.to_csv(index=False).encode("utf-8-sig"),
                file_name="keyword_results.csv",
                mime="text/csv"
            )

            excel_buffer = BytesIO()
            df.to_excel(excel_buffer, index=False, engine="openpyxl")
            excel_buffer.seek(0)

            st.download_button(
                "⬇ Excel 다운로드",
                data=excel_buffer,
                file_name="keyword_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# --------------------------------------------------
# 출처 검색
# --------------------------------------------------
elif mode == "출처 검색":
    st.subheader("출처 검색 (책 약자 + 페이지 번호)")

    search_lang_label = st.sidebar.selectbox(
        "검색 범위",
        ["영문(EN)", "한글(KO)", "통합(BOTH)"],
        index=2,
        key="cit_lang_label"
    )
    search_lang = {"영문(EN)": "EN", "한글(KO)": "KO", "통합(BOTH)": "BOTH"}[search_lang_label]

    limit_dates = st.sidebar.slider("최대 결과 날짜 수", 10, 300, 50, 10, key="cit_limit")

    selected_short = st.selectbox("책(약자)", ALL_BOOKS, index=0, key="cit_book")
    selected_full = BOOK_MAP.get(selected_short, selected_short)
    st.caption(f"풀네임: {selected_full}")

    page = st.number_input("페이지(숫자)", min_value=1, max_value=9999, value=68, step=1, key="cit_page")

    if st.button("검색", key="cit_btn"):
        dates = citation_search_dates(selected_short, int(page), search_lang, limit_dates)
        st.write(f"결과: {len(dates)}개 날짜")

        download_rows = []

        for d in dates:
            en, ko = fetch_pair_by_date(d)

            download_rows.append({
                "date": d,
                "book_short": selected_short,
                "book_full": selected_full,
                "page": int(page),
                "EN_title": en["title"] if en else "",
                "EN_body": en["body"] if en else "",
                "KO_title": ko["title"] if ko else "",
                "KO_body": ko["body"] if ko else "",
            })

            if en:
                title_line = f"{d} | {en['title']} (EN:{en['volume_id']})"
                if ko:
                    title_line += f" ⇄ (KO:{ko['volume_id']})"
            elif ko:
                title_line = f"{d} | {ko['title']} (KO:{ko['volume_id']})"
            else:
                title_line = f"{d} | (no rows?)"

            with st.expander(title_line, expanded=False):
                left, right = st.columns(2)
                with left:
                    if en:
                        st.markdown(escape(en["body"]).replace("\n", "<br>"), unsafe_allow_html=True)
                    else:
                        st.info("EN: (없음)")
                with right:
                    if ko:
                        st.markdown(highlight_citation_only(ko["body"], selected_short, selected_full), unsafe_allow_html=True)
                    else:
                        st.info("KO: (없음)")

        if download_rows:
            df = pd.DataFrame(download_rows)

            st.download_button(
                "⬇ CSV 다운로드",
                df.to_csv(index=False).encode("utf-8-sig"),
                file_name="citation_results.csv",
                mime="text/csv"
            )

            excel_buffer = BytesIO()
            df.to_excel(excel_buffer, index=False, engine="openpyxl")
            excel_buffer.seek(0)

            st.download_button(
                "⬇ Excel 다운로드",
                data=excel_buffer,
                file_name="citation_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# --------------------------------------------------
# 책별 통계
# --------------------------------------------------
elif mode == "책별 통계":
    st.header("📊 책별 인용 통계 (KO 본문 기준)")
    st.caption("※ 하루에 같은 책이 2번 나오면 2회로 계산(등장 횟수 기준)")

    stats_type = st.radio("보기", ["전체 책 인용 순위", "특정 책 연도별 그래프"], key="stats_type")

    if stats_type == "전체 책 인용 순위":
        top_n = st.slider("Top N", 5, min(60, len(ALL_BOOKS)), 20, 5, key="stats_topn")
        counts = stats_count_all_citations_ko()
        items = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        items = [(k, v) for k, v in items if v > 0]
        show = items[:top_n]

        for short, cnt in show:
            st.write(f"- **{short}** ({BOOK_MAP[short]}) : **{cnt}회**")

        if HAS_MPL and show:
            labels = [k for k, _ in show]
            values = [v for _, v in show]
            fig = plt.figure()
            plt.bar(labels, values)
            plt.xlabel("책(약자)")
            plt.ylabel("인용 횟수")
            plt.xticks(rotation=60, ha="right")
            st.pyplot(fig)

    else:
        selected_book = st.selectbox("책 선택(약자)", ALL_BOOKS, key="stats_book")
        year_counts = stats_count_book_by_year_ko(selected_book)
        years = sorted(year_counts.keys())
        values = [year_counts[y] for y in years]

        if not years:
            st.info("이 책은 KO 본문에서 인용이 0회입니다.")
        else:
            st.dataframe({"Year": years, "Count": values}, use_container_width=True)
            if HAS_MPL:
                fig = plt.figure()
                plt.plot(years, values)
                plt.xlabel("연도")
                plt.ylabel("인용 횟수")
                st.pyplot(fig)

# --------------------------------------------------
# 페이지 탐색기
# --------------------------------------------------
else:
    st.header("📖 페이지 탐색기 (책 페이지별 인용 + 본문 열람)")
    st.caption("페이지 분포는 Top N으로 제한 가능 / 열람 결과는 항상 해당 날짜 KO 본문 전체 표시")

    book = st.selectbox("책 선택(약자)", ALL_BOOKS, key="pe_book")
    full = BOOK_MAP[book]
    dist = page_distribution_ko(book)  # {page: count}

    if not dist:
        st.info("이 책의 인용 페이지가 KO 본문에서 발견되지 않았습니다.")
    else:
        sort_mode = st.radio(
            "정렬 기준",
            ["페이지 번호 순", "인용 많은 순"],
            horizontal=True,
            key="pe_sort"
        )

        max_top = min(1000, len(dist))
        top_n = st.slider("Top N (표/그래프 표시 개수)", 10, max_top, min(200, max_top), 10, key="pe_topn")

        if sort_mode == "인용 많은 순":
            items_all = sorted(dist.items(), key=lambda x: x[1], reverse=True)
        else:
            items_all = sorted(dist.items(), key=lambda x: x[0])

        items_view = items_all[:top_n]

        st.subheader("페이지 분포 (Top N)")
        st.dataframe(
            {"Page": [p for p, _ in items_view], "Count": [c for _, c in items_view]},
            use_container_width=True
        )

        if HAS_MPL:
            fig = plt.figure()
            plt.bar([str(p) for p, _ in items_view], [c for _, c in items_view])
            plt.xlabel("Page")
            plt.ylabel("Citation Count")
            plt.xticks(rotation=60, ha="right")
            st.pyplot(fig)

        pages_sorted = sorted(dist.keys())

        if st.session_state.get("pe_book_prev") != book:
            st.session_state.pe_book_prev = book
            st.session_state.pe_cur_page = pages_sorted[0]

        if "pe_cur_page" not in st.session_state or st.session_state.pe_cur_page not in pages_sorted:
            st.session_state.pe_cur_page = pages_sorted[0]

        cur_idx = pages_sorted.index(st.session_state.pe_cur_page)

        c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 1])
        with c1:
            if st.button("⏮ 처음", use_container_width=True, key="pe_first"):
                st.session_state.pe_cur_page = pages_sorted[0]
                st.rerun()
        with c2:
            if st.button("◀ -10", use_container_width=True, key="pe_m10"):
                st.session_state.pe_cur_page = pages_sorted[max(0, cur_idx - 10)]
                st.rerun()
        with c3:
            if st.button("◀ 이전", use_container_width=True, key="pe_prev"):
                st.session_state.pe_cur_page = pages_sorted[max(0, cur_idx - 1)]
                st.rerun()
        with c4:
            if st.button("다음 ▶", use_container_width=True, key="pe_next"):
                st.session_state.pe_cur_page = pages_sorted[min(len(pages_sorted) - 1, cur_idx + 1)]
                st.rerun()
        with c5:
            if st.button("+10 ▶", use_container_width=True, key="pe_p10"):
                st.session_state.pe_cur_page = pages_sorted[min(len(pages_sorted) - 1, cur_idx + 10)]
                st.rerun()

        if st.button("⏭ 끝", use_container_width=True, key="pe_last"):
            st.session_state.pe_cur_page = pages_sorted[-1]
            st.rerun()

        st.selectbox("열람할 페이지(번호순)", pages_sorted, key="pe_cur_page")
        page = int(st.session_state.pe_cur_page)

        st.subheader(f"페이지 {page} 열람 결과 (KO 본문 전체)")

        rows = conn.execute(
            """
            SELECT d, title, body
            FROM devotions
            WHERE volume_id LIKE 'KO_%'
            ORDER BY d ASC
            """
        ).fetchall()

        hit_count = 0
        hit_rows = []

        for r in rows:
            d = r["d"]
            title = r["title"]
            body = r["body"] or ""
            if not body:
                continue

            if body_has_citation(body, short=book, full=full, page=page):
                hit_count += 1
                hit_rows.append({"date": d, "title": title, "body": body})

                with st.expander(f"{d} | {title}", expanded=False):
                    st.markdown(highlight_citation_only(body, book, full), unsafe_allow_html=True)

        st.write(f"총 {hit_count}개 날짜에서 페이지 {page}가 인용되었습니다.")

        # 페이지 탐색기 다운로드
        if hit_rows:
            df = pd.DataFrame(hit_rows)

            st.download_button(
                "⬇ CSV 다운로드 (현재 페이지 결과)",
                df.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"page_explorer_{book}_{page}.csv",
                mime="text/csv"
            )

            excel_buffer = BytesIO()
            df.to_excel(excel_buffer, index=False, engine="openpyxl")
            excel_buffer.seek(0)

            st.download_button(
                "⬇ Excel 다운로드 (현재 페이지 결과)",
                data=excel_buffer,
                file_name=f"page_explorer_{book}_{page}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

from io import BytesIO
import pandas as pd
import streamlit as st
import sqlite3
import re
from pathlib import Path
from html import escape
import calendar
from collections import defaultdict

# --------------------------------------------------
# matplotlib optional (없어도 앱은 돌아가되, 그래프만 비활성)
# --------------------------------------------------
try:
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm  # noqa: F401

    plt.rcParams["font.family"] = "Malgun Gothic"
    plt.rcParams["axes.unicode_minus"] = False
    HAS_MPL = True
except Exception:
    HAS_MPL = False

# --------------------------------------------------
# DB
# --------------------------------------------------
DB_PATH = Path("data/egw_devotionals.sqlite")

st.set_page_config(
    page_title="EGW 기도력 검색",
    page_icon="📖",
    layout="wide"
)
st.title("📖 EGW 기도력 검색 (영문/한글)")

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# --------------------------------------------------
# 📚 약자 → 풀네임 매핑 (전체)
# --------------------------------------------------
BOOK_MAP = {
    "1설교": "설교와 강연 1권",
    "2설교": "설교와 강연 2권",
    "1기별": "가려 뽑은 기별 1권",
    "2기별": "가려 뽑은 기별 2권",
    "3기별": "가려 뽑은 기별 3권",
    "1보감": "증언보감 1권",
    "2보감": "증언보감 2권",
    "3보감": "증언보감 3권",
    "1증언": "교회증언 1권",
    "2증언": "교회증언 2권",
    "3증언": "교회증언 3권",
    "4증언": "교회증언 4권",
    "5증언": "교회증언 5권",
    "6증언": "교회증언 6권",
    "7증언": "교회증언 7권",
    "8증언": "교회증언 8권",
    "9증언": "교회증언 9권",
    "가건": "가정과 건강",
    "가정": "재림신도의 가정",
    "교권": "교회에 보내는 권면",
    "교육": "교육",
    "구호": "구호봉사",
    "기도": "기도",
    "높은 부르심": "우리의 높은 부르심",
    "대결": "대결",
    "도시": "도시 선교",
    "딸들": "하나님의 딸들",
    "동행": "동행",
    "리더십": "그리스도인 리더십",
    "1마음": "그리스도인의 마음과 품성과 인격 1권",
    "2마음": "그리스도인의 마음과 품성과 인격 2권",
    "목사": "목사와 복음 교역자에게 보내는 증언",
    "목회": "목회봉사",
    "문선": "그리스도인 문서 선교",
    "문전": "문서전도봉사",
    "믿음": "믿음과 행함",
    "바울": "바울의 생애",
    "보훈": "산상보훈",
    "복음": "복음 교역자",
    "부모": "부모와 교사와 학생에게 보내는 권면",
    "부조": "부조와 선지자",
    "사건": "마지막 날 사건들",
    "살아": "살아남는 이들",
    "생애": "생애의 빛",
    "선교": "그리스도인 선교봉사",
    "선지": "선지자와 왕",
    "성소": "성소에 계신 그리스도",
    "성화": "성화된 생애",
    "소망": "시대의 소망",
    "실물": "실물교훈",
    "안교": "안식일학교 사업에 관한 권면",
    "음식": "식생활과 음식물에 관한 권면",
    "의료": "의료봉사",
    "인류": "인류의 빛",
    "자녀": "새 자녀 지도법",
    "자서": "엘렌 G. 화잇 자서전",
    "쟁투": "각 시대의 대쟁투",
    "전도": "복음전도",
    "절제": "절제생활",
    "정로": "정로의 계단",
    "청년": "청년에게 보내는 기별",
    "청지기": "청지기에게 보내는 권면",
    "초기": "초기문집",
    "치료": "치료봉사",
    "하늘": "하늘",
    "행실": "성적 행실과 간음과 이혼에 관한 증언",
    "행적": "사도행적",
    "화잇주석": "엘렌 G. 화잇의 주석",
}
ALL_BOOKS = sorted(BOOK_MAP.keys())


def build_where_lang(search_lang: str) -> str:
    if search_lang == "EN":
        return "volume_id LIKE 'EN_%'"
    if search_lang == "KO":
        return "volume_id LIKE 'KO_%'"
    return "(volume_id LIKE 'EN_%' OR volume_id LIKE 'KO_%')"


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def highlight_phrase_only(text: str, phrase: str) -> str:
    if not text:
        return ""
    raw_html = escape(text).replace("\n", "<br>")
    p = (phrase or "").strip()
    if not p:
        return raw_html
    try:
        rx = re.compile(re.escape(p), re.IGNORECASE)
        return rx.sub(r"<mark>\g<0></mark>", raw_html)
    except re.error:
        return raw_html


def highlight_words(text: str, query: str) -> str:
    if not text:
        return ""
    raw_html = escape(text).replace("\n", "<br>")
    q = (query or "").strip()
    if not q:
        return raw_html

    parts = [p for p in re.split(r"\s+", q) if p.strip()]
    parts = [p.strip("()[]{}<>.,;:\"'“”‘’") for p in parts]
    parts = [p for p in parts if p]
    if not parts:
        return raw_html

    pattern = "(" + "|".join(re.escape(p) for p in sorted(set(parts), key=len, reverse=True)) + ")"
    try:
        rx = re.compile(pattern, re.IGNORECASE)
        return rx.sub(r"<mark>\1</mark>", raw_html)
    except re.error:
        return raw_html


def fetch_pair_by_date(d: str):
    en = conn.execute(
        """
        SELECT d, title, body, volume_id
        FROM devotions
        WHERE d=? AND volume_id LIKE 'EN_%'
        ORDER BY volume_id
        LIMIT 1
        """,
        (d,),
    ).fetchone()

    ko = conn.execute(
        """
        SELECT d, title, body, volume_id
        FROM devotions
        WHERE d=? AND volume_id LIKE 'KO_%'
        ORDER BY volume_id
        LIMIT 1
        """,
        (d,),
    ).fetchone()

    return en, ko


# --------------------------------------------------
# Keyword Search
# --------------------------------------------------
def tokenize_simple(q: str) -> list[str]:
    q = (q or "").strip()
    if not q:
        return []
    toks = []
    for t in re.split(r"\s+", q):
        t = t.strip("()[]{}<>.,;:\"'“”‘’")
        if t:
            toks.append(t)
    return toks[:12]


def keyword_search_dates_phrase(query: str, search_lang: str, limit_dates: int) -> list[str]:
    where_lang = build_where_lang(search_lang)
    q = (query or "").strip()
    if not q:
        return []
    sql = f"""
    SELECT d
    FROM devotions
    WHERE body LIKE ?
      AND {where_lang}
    GROUP BY d
    ORDER BY d DESC
    LIMIT ?
    """
    rows = conn.execute(sql, (f"%{q}%", limit_dates)).fetchall()
    return [r["d"] for r in rows]


def keyword_search_dates_tokens_and(query: str, search_lang: str, limit_dates: int) -> list[str]:
    where_lang = build_where_lang(search_lang)
    tokens = tokenize_simple(query)
    if not tokens:
        return []

    conds = []
    params = []
    for t in tokens:
        conds.append("body LIKE ?")
        params.append(f"%{t}%")

    sql = f"""
    SELECT d
    FROM devotions
    WHERE {" AND ".join(conds)}
      AND {where_lang}
    GROUP BY d
    ORDER BY d DESC
    LIMIT ?
    """
    params.append(limit_dates)
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [r["d"] for r in rows]


# --------------------------------------------------
# Citation Search
# --------------------------------------------------
def page_in_pagespec_exact(page: int, pagespec: str) -> bool:
    if not pagespec:
        return False
    s = pagespec.replace("–", "-").replace("—", "-").replace("~", "-")
    s = re.sub(r"\s+", " ", s)

    for a, b in re.findall(r"(\d{1,4})\s*-\s*(\d{1,4})", s):
        lo = int(a)
        hi = int(b)
        if lo <= page <= hi or hi <= page <= lo:
            return True

    page_pat = re.compile(r"(?<!\d)" + re.escape(str(page)) + r"(?!\d)")
    return bool(page_pat.search(s))


def body_has_citation(body: str, short: str, full: str, page: int) -> bool:
    if not body:
        return False

    paren_rx = re.compile(r"[（(]\s*" + re.escape(short) + r"\s*[,，]\s*([^）)]+)[）)]")
    for m in paren_rx.finditer(body):
        if page_in_pagespec_exact(page, m.group(1)):
            return True

    bracket_rx = re.compile(r"『\s*" + re.escape(full) + r"\s*』\s*[,，]?\s*([0-9][0-9\s,\-~–—]+)")
    for m in bracket_rx.finditer(body):
        if page_in_pagespec_exact(page, m.group(1)):
            return True

    return False


def citation_search_dates(short: str, page: int, search_lang: str, limit_dates: int = 50) -> list[str]:
    where_lang = build_where_lang(search_lang)
    full = BOOK_MAP.get(short, short)
    page_str = str(page)

    sql = f"""
    SELECT d, body
    FROM devotions
    WHERE {where_lang}
      AND (body LIKE ? OR body LIKE ?)
      AND body LIKE ?
    ORDER BY d DESC
    LIMIT 3000
    """
    rows = conn.execute(sql, (f"%{short}%", f"%{full}%", f"%{page_str}%")).fetchall()

    dates = []
    seen = set()
    for r in rows:
        d = r["d"]
        if d in seen:
            continue
        if body_has_citation(r["body"], short=short, full=full, page=page):
            seen.add(d)
            dates.append(d)
        if len(dates) >= limit_dates:
            break
    return dates


def highlight_citation_only(body: str, short: str, full: str) -> str:
    if not body:
        return ""
    html = escape(body).replace("\n", "<br>")

    paren_rx = re.compile(r"([（(]\s*" + re.escape(short) + r"\s*[,，]\s*[^）)]+[）)])")
    html = paren_rx.sub(r"<mark>\1</mark>", html)

    bracket_rx = re.compile(r"(『\s*" + re.escape(full) + r"\s*』\s*[,，]?\s*[0-9][0-9\s,\-~–—]+)")
    html = bracket_rx.sub(r"<mark>\1</mark>", html)

    return html


# --------------------------------------------------
# Book Stats (KO 기준)
# --------------------------------------------------
def _compile_book_regex():
    compiled = {}
    for short, full in BOOK_MAP.items():
        paren = re.compile(r"[（(]\s*" + re.escape(short) + r"\s*[,，]\s*[^）)]+[）)]")
        bracket = re.compile(r"『\s*" + re.escape(full) + r"\s*』\s*[,，]?\s*[0-9][0-9\s,\-~–—]+")
        compiled[short] = (paren, bracket)
    return compiled


BOOK_RX = _compile_book_regex()


@st.cache_data(show_spinner=False)
def stats_count_all_citations_ko():
    counts = defaultdict(int)
    rows = conn.execute(
        """
        SELECT body
        FROM devotions
        WHERE volume_id LIKE 'KO_%'
        """
    ).fetchall()

    for r in rows:
        body = r["body"] or ""
        if not body:
            continue
        for short, (rx1, rx2) in BOOK_RX.items():
            counts[short] += len(rx1.findall(body))
            counts[short] += len(rx2.findall(body))
    return dict(counts)


@st.cache_data(show_spinner=False)
def stats_count_book_by_year_ko(selected_short: str):
    year_counts = defaultdict(int)
    rx1, rx2 = BOOK_RX[selected_short]

    rows = conn.execute(
        """
        SELECT d, body
        FROM devotions
        WHERE volume_id LIKE 'KO_%'
        """
    ).fetchall()

    for r in rows:
        body = r["body"] or ""
        if not body:
            continue
        year = (r["d"] or "")[:4]
        if not year:
            continue
        year_counts[year] += len(rx1.findall(body))
        year_counts[year] += len(rx2.findall(body))
    return dict(year_counts)


# --------------------------------------------------
# Page Explorer helpers
# --------------------------------------------------
def split_paragraphs(text: str) -> list[str]:
    parts = re.split(r"\n\s*\n+", text or "")
    return [p.strip() for p in parts if p.strip()]


def parse_pagespec_to_pages(pagespec: str, max_expand: int = 200) -> list[int]:
    if not pagespec:
        return []
    s = pagespec.replace("–", "-").replace("—", "-").replace("~", "-")
    s = s.replace("，", ",")
    s = re.sub(r"\s+", " ", s).strip()

    pages = []
    chunks = [c.strip() for c in s.split(",") if c.strip()]
    for c in chunks:
        m = re.match(r"^(\d{1,4})\s*-\s*(\d{1,4})$", c)
        if m:
            a = int(m.group(1))
            b = int(m.group(2))
            lo, hi = (a, b) if a <= b else (b, a)
            length = hi - lo + 1
            if length <= max_expand:
                pages.extend(list(range(lo, hi + 1)))
            else:
                pages.append(lo)
                pages.append(hi)
        else:
            m2 = re.match(r"^(\d{1,4})$", c)
            if m2:
                pages.append(int(m2.group(1)))

    return pages


def extract_pages_for_book(body: str, short: str, full: str) -> list[int]:
    pages = []

    paren_rx = re.compile(r"[（(]\s*" + re.escape(short) + r"\s*[,，]\s*([^）)]+)[）)]")
    for m in paren_rx.finditer(body or ""):
        pages.extend(parse_pagespec_to_pages(m.group(1)))

    bracket_rx = re.compile(r"『\s*" + re.escape(full) + r"\s*』\s*[,，]?\s*([0-9][0-9\s,\-~–—]+)")
    for m in bracket_rx.finditer(body or ""):
        pages.extend(parse_pagespec_to_pages(m.group(1)))

    return pages


@st.cache_data(show_spinner=False)
def page_distribution_ko(short: str):
    full = BOOK_MAP.get(short, short)
    counts = defaultdict(int)

    rows = conn.execute(
        """
        SELECT body
        FROM devotions
        WHERE volume_id LIKE 'KO_%'
        """
    ).fetchall()

    for r in rows:
        body = r["body"] or ""
        if not body:
            continue
        pages = extract_pages_for_book(body, short, full)
        for p in pages:
            counts[p] += 1

    return dict(counts)


# --------------------------------------------------
# UI
# --------------------------------------------------



# --------------------------------------------------
# 날짜 검색
# --------------------------------------------------
if mode == "날짜 검색":
    years = conn.execute("""
        SELECT DISTINCT substr(d,1,4) as year
        FROM devotions
        ORDER BY year
    """).fetchall()
    year_list = [int(r["year"]) for r in years]

    selected_year = st.selectbox("연도", year_list, key="date_year")
    selected_month = st.selectbox("월", list(range(1, 13)), key="date_month")
    last_day = calendar.monthrange(selected_year, selected_month)[1]
    selected_day = st.selectbox("일", list(range(1, last_day + 1)), key="date_day")

    d = f"{selected_year:04d}-{selected_month:02d}-{selected_day:02d}"

    if st.button("검색", key="date_search_btn"):
        en, ko = fetch_pair_by_date(d)

        if not en and not ko:
            st.warning("해당 날짜의 항목이 없습니다.")
        else:
            left, right = st.columns(2)
            with left:
                if en:
                    st.subheader(f"EN | {en['d']} | {en['title']} ({en['volume_id']})")
                    st.markdown(escape(en["body"]).replace("\n", "<br>"), unsafe_allow_html=True)
                else:
                    st.info("EN: (없음)")
            with right:
                if ko:
                    st.subheader(f"KO | {ko['d']} | {ko['title']} ({ko['volume_id']})")
                    st.markdown(escape(ko["body"]).replace("\n", "<br>"), unsafe_allow_html=True)
                else:
                    st.info("KO: (없음)")

# --------------------------------------------------
# 키워드 검색
# --------------------------------------------------
elif mode == "키워드 검색":
    query = st.text_area("키워드 또는 문장 입력 (EN 또는 KO)", height=120, key="kw_query")

    search_lang_label = st.sidebar.selectbox(
        "검색 범위",
        ["영문(EN)", "한글(KO)", "통합(BOTH)"],
        index=2,
        key="kw_lang_label"
    )
    search_lang = {"영문(EN)": "EN", "한글(KO)": "KO", "통합(BOTH)": "BOTH"}[search_lang_label]

    limit_dates = st.sidebar.slider("최대 결과 날짜 수", 10, 300, 50, 10, key="kw_limit")
    phrase_mode = st.checkbox("문구 그대로만 찾기(정확 검색)", value=True, key="kw_phrase")
    highlight_on = st.sidebar.checkbox("강조 표시(하이라이트)", value=True, key="kw_hl")

    if query.strip():
        dates = keyword_search_dates_phrase(query, search_lang, limit_dates) if phrase_mode else keyword_search_dates_tokens_and(query, search_lang, limit_dates)
        st.write(f"결과: {len(dates)}개 날짜")

        download_rows = []

        for d in dates:
            en, ko = fetch_pair_by_date(d)

            download_rows.append({
                "date": d,
                "EN_title": en["title"] if en else "",
                "EN_body": en["body"] if en else "",
                "KO_title": ko["title"] if ko else "",
                "KO_body": ko["body"] if ko else "",
            })

            if en:
                title_line = f"{d} | {en['title']} (EN:{en['volume_id']})"
                if ko:
                    title_line += f" ⇄ (KO:{ko['volume_id']})"
            elif ko:
                title_line = f"{d} | {ko['title']} (KO:{ko['volume_id']})"
            else:
                title_line = f"{d} | (no rows?)"

            with st.expander(title_line, expanded=False):
                left, right = st.columns(2)
                with left:
                    if en:
                        body_html = highlight_phrase_only(en["body"], query) if (highlight_on and phrase_mode) else (
                            highlight_words(en["body"], query) if highlight_on else escape(en["body"]).replace("\n", "<br>")
                        )
                        st.markdown(body_html, unsafe_allow_html=True)
                    else:
                        st.info("EN: (없음)")
                with right:
                    if ko:
                        body_html = highlight_phrase_only(ko["body"], query) if (highlight_on and phrase_mode) else (
                            highlight_words(ko["body"], query) if highlight_on else escape(ko["body"]).replace("\n", "<br>")
                        )
                        st.markdown(body_html, unsafe_allow_html=True)
                    else:
                        st.info("KO: (없음)")

        # 다운로드 버튼 (for 밖, if query 안)
        if download_rows:
            df = pd.DataFrame(download_rows)

            st.download_button(
                "⬇ CSV 다운로드",
                df.to_csv(index=False).encode("utf-8-sig"),
                file_name="keyword_results.csv",
                mime="text/csv"
            )

            excel_buffer = BytesIO()
            df.to_excel(excel_buffer, index=False, engine="openpyxl")
            excel_buffer.seek(0)

            st.download_button(
                "⬇ Excel 다운로드",
                data=excel_buffer,
                file_name="keyword_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# --------------------------------------------------
# 출처 검색
# --------------------------------------------------
elif mode == "출처 검색":
    st.subheader("출처 검색 (책 약자 + 페이지 번호)")

    search_lang_label = st.sidebar.selectbox(
        "검색 범위",
        ["영문(EN)", "한글(KO)", "통합(BOTH)"],
        index=2,
        key="cit_lang_label"
    )
    search_lang = {"영문(EN)": "EN", "한글(KO)": "KO", "통합(BOTH)": "BOTH"}[search_lang_label]

    limit_dates = st.sidebar.slider("최대 결과 날짜 수", 10, 300, 50, 10, key="cit_limit")

    selected_short = st.selectbox("책(약자)", ALL_BOOKS, index=0, key="cit_book")
    selected_full = BOOK_MAP.get(selected_short, selected_short)
    st.caption(f"풀네임: {selected_full}")

    page = st.number_input("페이지(숫자)", min_value=1, max_value=9999, value=68, step=1, key="cit_page")

    if st.button("검색", key="cit_btn"):
        dates = citation_search_dates(selected_short, int(page), search_lang, limit_dates)
        st.write(f"결과: {len(dates)}개 날짜")

        download_rows = []

        for d in dates:
            en, ko = fetch_pair_by_date(d)

            download_rows.append({
                "date": d,
                "book_short": selected_short,
                "book_full": selected_full,
                "page": int(page),
                "EN_title": en["title"] if en else "",
                "EN_body": en["body"] if en else "",
                "KO_title": ko["title"] if ko else "",
                "KO_body": ko["body"] if ko else "",
            })

            if en:
                title_line = f"{d} | {en['title']} (EN:{en['volume_id']})"
                if ko:
                    title_line += f" ⇄ (KO:{ko['volume_id']})"
            elif ko:
                title_line = f"{d} | {ko['title']} (KO:{ko['volume_id']})"
            else:
                title_line = f"{d} | (no rows?)"

            with st.expander(title_line, expanded=False):
                left, right = st.columns(2)
                with left:
                    if en:
                        st.markdown(escape(en["body"]).replace("\n", "<br>"), unsafe_allow_html=True)
                    else:
                        st.info("EN: (없음)")
                with right:
                    if ko:
                        st.markdown(highlight_citation_only(ko["body"], selected_short, selected_full), unsafe_allow_html=True)
                    else:
                        st.info("KO: (없음)")

        if download_rows:
            df = pd.DataFrame(download_rows)

            st.download_button(
                "⬇ CSV 다운로드",
                df.to_csv(index=False).encode("utf-8-sig"),
                file_name="citation_results.csv",
                mime="text/csv"
            )

            excel_buffer = BytesIO()
            df.to_excel(excel_buffer, index=False, engine="openpyxl")
            excel_buffer.seek(0)

            st.download_button(
                "⬇ Excel 다운로드",
                data=excel_buffer,
                file_name="citation_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# --------------------------------------------------
# 책별 통계
# --------------------------------------------------
elif mode == "책별 통계":
    st.header("📊 책별 인용 통계 (KO 본문 기준)")
    st.caption("※ 하루에 같은 책이 2번 나오면 2회로 계산(등장 횟수 기준)")

    stats_type = st.radio("보기", ["전체 책 인용 순위", "특정 책 연도별 그래프"], key="stats_type")

    if stats_type == "전체 책 인용 순위":
        top_n = st.slider("Top N", 5, min(60, len(ALL_BOOKS)), 20, 5, key="stats_topn")
        counts = stats_count_all_citations_ko()
        items = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        items = [(k, v) for k, v in items if v > 0]
        show = items[:top_n]

        for short, cnt in show:
            st.write(f"- **{short}** ({BOOK_MAP[short]}) : **{cnt}회**")

        if HAS_MPL and show:
            labels = [k for k, _ in show]
            values = [v for _, v in show]
            fig = plt.figure()
            plt.bar(labels, values)
            plt.xlabel("책(약자)")
            plt.ylabel("인용 횟수")
            plt.xticks(rotation=60, ha="right")
            st.pyplot(fig)

    else:
        selected_book = st.selectbox("책 선택(약자)", ALL_BOOKS, key="stats_book")
        year_counts = stats_count_book_by_year_ko(selected_book)
        years = sorted(year_counts.keys())
        values = [year_counts[y] for y in years]

        if not years:
            st.info("이 책은 KO 본문에서 인용이 0회입니다.")
        else:
            st.dataframe({"Year": years, "Count": values}, use_container_width=True)
            if HAS_MPL:
                fig = plt.figure()
                plt.plot(years, values)
                plt.xlabel("연도")
                plt.ylabel("인용 횟수")
                st.pyplot(fig)

# --------------------------------------------------
# 페이지 탐색기
# --------------------------------------------------
else:
    st.header("📖 페이지 탐색기 (책 페이지별 인용 + 본문 열람)")
    st.caption("페이지 분포는 Top N으로 제한 가능 / 열람 결과는 항상 해당 날짜 KO 본문 전체 표시")

    book = st.selectbox("책 선택(약자)", ALL_BOOKS, key="pe_book")
    full = BOOK_MAP[book]
    dist = page_distribution_ko(book)  # {page: count}

    if not dist:
        st.info("이 책의 인용 페이지가 KO 본문에서 발견되지 않았습니다.")
    else:
        sort_mode = st.radio(
            "정렬 기준",
            ["페이지 번호 순", "인용 많은 순"],
            horizontal=True,
            key="pe_sort"
        )

        max_top = min(1000, len(dist))
        top_n = st.slider("Top N (표/그래프 표시 개수)", 10, max_top, min(200, max_top), 10, key="pe_topn")

        if sort_mode == "인용 많은 순":
            items_all = sorted(dist.items(), key=lambda x: x[1], reverse=True)
        else:
            items_all = sorted(dist.items(), key=lambda x: x[0])

        items_view = items_all[:top_n]

        st.subheader("페이지 분포 (Top N)")
        st.dataframe(
            {"Page": [p for p, _ in items_view], "Count": [c for _, c in items_view]},
            use_container_width=True
        )

        if HAS_MPL:
            fig = plt.figure()
            plt.bar([str(p) for p, _ in items_view], [c for _, c in items_view])
            plt.xlabel("Page")
            plt.ylabel("Citation Count")
            plt.xticks(rotation=60, ha="right")
            st.pyplot(fig)

        pages_sorted = sorted(dist.keys())

        if st.session_state.get("pe_book_prev") != book:
            st.session_state.pe_book_prev = book
            st.session_state.pe_cur_page = pages_sorted[0]

        if "pe_cur_page" not in st.session_state or st.session_state.pe_cur_page not in pages_sorted:
            st.session_state.pe_cur_page = pages_sorted[0]

        cur_idx = pages_sorted.index(st.session_state.pe_cur_page)

        c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 1])
        with c1:
            if st.button("⏮ 처음", use_container_width=True, key="pe_first"):
                st.session_state.pe_cur_page = pages_sorted[0]
                st.rerun()
        with c2:
            if st.button("◀ -10", use_container_width=True, key="pe_m10"):
                st.session_state.pe_cur_page = pages_sorted[max(0, cur_idx - 10)]
                st.rerun()
        with c3:
            if st.button("◀ 이전", use_container_width=True, key="pe_prev"):
                st.session_state.pe_cur_page = pages_sorted[max(0, cur_idx - 1)]
                st.rerun()
        with c4:
            if st.button("다음 ▶", use_container_width=True, key="pe_next"):
                st.session_state.pe_cur_page = pages_sorted[min(len(pages_sorted) - 1, cur_idx + 1)]
                st.rerun()
        with c5:
            if st.button("+10 ▶", use_container_width=True, key="pe_p10"):
                st.session_state.pe_cur_page = pages_sorted[min(len(pages_sorted) - 1, cur_idx + 10)]
                st.rerun()

        if st.button("⏭ 끝", use_container_width=True, key="pe_last"):
            st.session_state.pe_cur_page = pages_sorted[-1]
            st.rerun()

        st.selectbox("열람할 페이지(번호순)", pages_sorted, key="pe_cur_page")
        page = int(st.session_state.pe_cur_page)

        st.subheader(f"페이지 {page} 열람 결과 (KO 본문 전체)")

        rows = conn.execute(
            """
            SELECT d, title, body
            FROM devotions
            WHERE volume_id LIKE 'KO_%'
            ORDER BY d ASC
            """
        ).fetchall()

        hit_count = 0
        hit_rows = []

        for r in rows:
            d = r["d"]
            title = r["title"]
            body = r["body"] or ""
            if not body:
                continue

            if body_has_citation(body, short=book, full=full, page=page):
                hit_count += 1
                hit_rows.append({"date": d, "title": title, "body": body})

                with st.expander(f"{d} | {title}", expanded=False):
                    st.markdown(highlight_citation_only(body, book, full), unsafe_allow_html=True)

        st.write(f"총 {hit_count}개 날짜에서 페이지 {page}가 인용되었습니다.")

        # 페이지 탐색기 다운로드
        if hit_rows:
            df = pd.DataFrame(hit_rows)

            st.download_button(
                "⬇ CSV 다운로드 (현재 페이지 결과)",
                df.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"page_explorer_{book}_{page}.csv",
                mime="text/csv"
            )

            excel_buffer = BytesIO()
            df.to_excel(excel_buffer, index=False, engine="openpyxl")
            excel_buffer.seek(0)

            st.download_button(
                "⬇ Excel 다운로드 (현재 페이지 결과)",
                data=excel_buffer,
                file_name=f"page_explorer_{book}_{page}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )



