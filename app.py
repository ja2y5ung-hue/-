"""
국비훈련 통합 관리 시스템
- 연간개설계획 마스터 DB 분석
- 모집현황 텍스트 파싱 (AI 없이 패턴 매칭)
- 과정별 이수자평가 / 비용신청 / 취업성과 / 만족도 추적
- Google Sheets 저장 (팀 공유)
"""
import streamlit as st
import openpyxl
import pandas as pd
import re
import os
import difflib
from datetime import datetime
from io import BytesIO

# ── Google Sheets (선택적 연동) ──────────────────
def get_gsheet():
    """Google Sheets 연결. secrets 없으면 None 반환"""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        gc = gspread.authorize(creds)
        return gc.open_by_key(st.secrets["sheet_id"])
    except Exception:
        return None

SHEET_COLS = [
    "key","계열","지점","훈련종류","과정명","시작일","종료일","정원",
    "기준주차","확정인원","신청인원","모집률","신청률",
    "개설상태","연기사유","모집비고",
    "평가완료","평가완료일","평가비고",
    "비용신청","비용금액","비용신청일","비용비고",
    "취업_이수자","취업_취업자","취업_조사일","취업비고",
    "만족도점수","만족도조사일","만족도비고",
    "업데이트",
]

@st.cache_data(ttl=30, show_spinner=False)
def load_gsheet_data(_sheet):
    """Google Sheets 전체 데이터 로드"""
    if _sheet is None:
        return {}
    try:
        try:
            ws = _sheet.worksheet("추적DB")
        except Exception:
            ws = _sheet.add_worksheet("추적DB", 1000, len(SHEET_COLS))
            ws.append_row(SHEET_COLS)
            return {}
        records = ws.get_all_records()
        return {r["key"]: r for r in records if r.get("key")}
    except Exception:
        return {}

def save_to_gsheet(sheet, row_dict):
    """Google Sheets에 행 저장 (upsert)"""
    if sheet is None:
        return False
    try:
        try:
            ws = sheet.worksheet("추적DB")
        except Exception:
            ws = sheet.add_worksheet("추적DB", 1000, len(SHEET_COLS))
            ws.append_row(SHEET_COLS)
        all_vals = ws.get_all_values()
        key = row_dict.get("key", "")
        new_row = [str(row_dict.get(c, "")) for c in SHEET_COLS]
        for i, row in enumerate(all_vals[1:], start=2):
            if row and row[0] == key:
                ws.update(f"A{i}", [new_row])
                return True
        ws.append_row(new_row)
        return True
    except Exception as e:
        st.toast(f"Sheets 저장 오류: {e}", icon="⚠️")
        return False

# ── 페이지 설정 ──────────────────────────────────
st.set_page_config(
    page_title="국비훈련 통합 관리",
    page_icon="📋",
    layout="wide",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;500;700&display=swap');
html,body,[class*="css"]{font-family:'Noto Sans KR',sans-serif;}
.page-title{font-size:1.55rem;font-weight:700;color:#1a365d;margin-bottom:0.1rem;}
.page-sub{font-size:0.85rem;color:#718096;margin-bottom:0.8rem;}
.kpi-box{background:white;border:1px solid #e2e8f0;border-radius:10px;
         padding:0.85rem 1rem;text-align:center;}
.kpi-num{font-size:1.7rem;font-weight:700;}
.kpi-label{font-size:0.76rem;color:#718096;margin-top:0.1rem;}
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="page-title">📋 국비훈련 통합 관리 시스템</p>', unsafe_allow_html=True)
st.markdown('<p class="page-sub">연간개설계획 · 모집현황 · 이수자평가 · 비용신청 · 취업성과 · 만족도 통합 관리</p>',
            unsafe_allow_html=True)

# ── Google Sheets 연결 상태 ──────────────────────
sheet = get_gsheet()
if sheet:
    st.sidebar.success("✅ Google Sheets 연결됨", icon="🔗")
else:
    st.sidebar.warning("Google Sheets 미연결\n\n데이터가 세션 내에서만 유지됩니다.", icon="⚠️")

# ── 연간개설계획 파일 로드 (자동 또는 업로드) ────────
AUTO_PLAN = "plan.xlsx.xlsx"  # GitHub에 올려둔 파일명

with st.sidebar:
    st.markdown("### 📂 연간개설계획 파일")
    if os.path.exists(AUTO_PLAN):
        st.success(f"✅ plan.xlsx 자동 로드됨", icon="📋")
        plan_file = AUTO_PLAN
        # 다른 파일로 교체 원할 때
        override = st.file_uploader("다른 파일로 교체", type=["xlsx","XLSX"])
        if override:
            plan_file = override
    else:
        plan_file = st.file_uploader(
            "엑셀 업로드 (.xlsx)",
            type=["xlsx", "XLSX"],
        )

if not plan_file:
    st.info("👈 왼쪽 사이드바에서 연간개설계획 엑셀 파일을 업로드해주세요.")
    st.stop()

# ── 헬퍼 함수 ────────────────────────────────────
def get_status(s, e, today):
    if not isinstance(s, datetime) or not isinstance(e, datetime):
        return "정보없음"
    return "완료" if e < today else ("진행중" if s <= today else "예정")

def get_venue(v):
    if not v or str(v).strip() in ("", "None"):
        return "미확인"
    u = str(v).strip().upper()
    return "미확보" if u == "N" else ("확보" if u in ["O","○","◯"] else f"확보({v})")

def classify_reason(r):
    if not r: return "기타"
    r = str(r)
    if "강의장" in r: return "강의장 부족"
    if "모집" in r and ("저조" in r or "부족" in r): return "모집률 저조"
    if "미개설" in r: return "미개설"
    if "강사" in r: return "강사 문제"
    if "효율" in r or "단가" in r: return "비용/효율성"
    if "직종" in r: return "직종 중복/조정"
    return "기타"

def course_key(지점, 과정명, 회차="1"):
    return f"{지점}|{과정명}|{회차}"

# ── 마스터 DB 파싱 ────────────────────────────────
@st.cache_data(show_spinner="파일 읽는 중...")
def parse_plan(file_bytes):
    wb    = openpyxl.load_workbook(BytesIO(file_bytes))
    today = datetime.now()
    ws1   = wb["1.연간개설계획"]
    courses = []
    for row in ws1.iter_rows(min_row=6, values_only=True):
        if not row[1]:
            continue
        s, e, v = row[9], row[10], row[16]
        courses.append({
            "연번": row[1], "계열": str(row[2] or ""), "지점": str(row[3] or ""),
            "훈련종류": str(row[4] or ""), "직종": str(row[5] or ""),
            "과정명": str(row[6] or ""), "운영회차": str(row[7] or "1"),
            "시작일": s.strftime("%Y-%m-%d") if isinstance(s, datetime) else str(s or ""),
            "종료일": e.strftime("%Y-%m-%d") if isinstance(e, datetime) else str(e or ""),
            "훈련일수": row[11], "훈련시간": row[12], "정원": int(row[15] or 0),
            "강의장": str(v or ""), "강사": str(row[17] or ""), "비고": str(row[18] or ""),
            "진행상태": get_status(s, e, today),
            "강의장상태": get_venue(v),
        })
    ws2 = wb["2.반납과정"]
    returns = []
    for row in ws2.iter_rows(min_row=6, values_only=True):
        if not row[1]:
            continue
        r = row[11]
        returns.append({
            "연번": row[1], "계열": str(row[2] or ""), "지점": str(row[3] or ""),
            "훈련종류": str(row[4] or ""), "직종": str(row[5] or ""),
            "과정명": str(row[6] or ""),
            "반납사유": str(r or ""), "사유분류": classify_reason(r),
        })
    return courses, returns

if isinstance(plan_file, str):
    with open(plan_file, "rb") as f:
        file_bytes = f.read()
else:
    file_bytes = plan_file.read()
courses, returns = parse_plan(file_bytes)

# ── Google Sheets 데이터 로드 (또는 session_state) ──
if sheet:
    db = load_gsheet_data(sheet)
else:
    if "local_db" not in st.session_state:
        st.session_state.local_db = {}
    db = st.session_state.local_db

def save_record(key, record):
    """저장 (Sheets 또는 session_state)"""
    if sheet:
        save_to_gsheet(sheet, record)
        st.cache_data.clear()
    else:
        st.session_state.local_db[key] = record

# ── 상단 KPI ─────────────────────────────────────
total      = len(courses)
in_prog    = sum(1 for c in courses if c["진행상태"] == "진행중")
scheduled  = sum(1 for c in courses if c["진행상태"] == "예정")
confirmed  = sum(1 for r in db.values() if str(r.get("개설상태","")) == "개강확정")
warn_cnt   = sum(1 for r in db.values()
                 if str(r.get("개설상태","")) == "개강확정"
                 and (float(r.get("모집률",1) or 1) < 0.65
                      or float(r.get("신청률",1) or 1) < 0.70))
delay_cnt  = sum(1 for r in db.values() if str(r.get("개설상태","")) == "개강연기")

kpi_cols = st.columns(6)
for col, num, lbl, clr in [
    (kpi_cols[0], total,     "전체 계획과정", "#2b6cb0"),
    (kpi_cols[1], in_prog,   "진행중",       "#276749"),
    (kpi_cols[2], scheduled, "예정",         "#2c5282"),
    (kpi_cols[3], confirmed, "개강확정",     "#276749"),
    (kpi_cols[4], warn_cnt,  "모집경고",     "#e53e3e"),
    (kpi_cols[5], delay_cnt, "개강연기",     "#c05621"),
]:
    with col:
        st.markdown(
            f'<div class="kpi-box"><div class="kpi-num" style="color:{clr}">{num}</div>'
            f'<div class="kpi-label">{lbl}</div></div>',
            unsafe_allow_html=True,
        )

st.markdown("<br>", unsafe_allow_html=True)

# ════════════════════════════════════════════════
# 탭 구성
# ════════════════════════════════════════════════
tab0, tab1, tab2, tab3, tab4 = st.tabs([
    "📋 개설 계획",
    "🎯 모집현황 입력",
    "📊 모집현황 조회",
    "🔍 과정 추적 관리",
    "🔴 반납 분석",
])

# ══════════════════════════════════════════════
# TAB 0 : 개설 계획
# ══════════════════════════════════════════════
with tab0:
    from collections import defaultdict

    def empty_stat():
        return {"수": 0, "정원": 0, "진행중": 0, "예정": 0, "완료": 0, "미확보": 0}

    def add_stat(d, c):
        d["수"] += 1
        d["정원"] += c["정원"]
        d[c["진행상태"]] = d.get(c["진행상태"], 0) + 1
        if c["강의장상태"] == "미확보":
            d["미확보"] += 1

    s_tot = defaultdict(empty_stat)
    s_br  = defaultdict(lambda: defaultdict(empty_stat))
    for c in courses:
        ser = c["계열"] or "미분류"
        br  = c["지점"] or "미분류"
        add_stat(s_tot[ser], c)
        add_stat(s_br[ser][br], c)

    s_tot = dict(sorted(s_tot.items()))
    s_br  = {k: dict(sorted(v.items())) for k, v in sorted(s_br.items())}

    G = "2fr 0.7fr 0.9fr 0.6fr 0.6fr 0.6fr 0.75fr"

    def th():
        return (
            f"<div style='display:grid;grid-template-columns:{G};background:#1a365d;color:white;"
            f"border-radius:6px 6px 0 0;padding:0.45rem 0.8rem;font-size:0.79rem;font-weight:700;gap:4px'>"
            "<span>구분</span><span style='text-align:center'>과정수</span>"
            "<span style='text-align:center'>정원합계</span><span style='text-align:center'>진행중</span>"
            "<span style='text-align:center'>예정</span><span style='text-align:center'>완료</span>"
            "<span style='text-align:center'>강의장미확보</span></div>"
        )

    def tr(label, d, bg, bold=False, indent=False):
        lc  = "#1a365d" if bold else "#4a5568"
        fw  = "700"     if bold else "400"
        pl  = "1.6rem"  if indent else "0.8rem"
        pre = "└ "      if indent else ""
        wc  = "color:#c05621;font-weight:700;" if d["미확보"] > 0 else ""
        return (
            f"<div style='display:grid;grid-template-columns:{G};background:{bg};"
            f"border:1px solid #e2e8f0;border-top:none;"
            f"padding:0.38rem 0.8rem 0.38rem {pl};font-size:0.81rem;gap:4px;align-items:center'>"
            f"<span style='color:{lc};font-weight:{fw}'>{pre}{label}</span>"
            f"<span style='text-align:center;font-weight:700;color:#2b6cb0'>{d['수']}</span>"
            f"<span style='text-align:center'>{d['정원']:,}명</span>"
            f"<span style='text-align:center;color:#276749;font-weight:600'>{d.get('진행중',0)}</span>"
            f"<span style='text-align:center;color:#2c5282'>{d.get('예정',0)}</span>"
            f"<span style='text-align:center;color:#718096'>{d.get('완료',0)}</span>"
            f"<span style='text-align:center;{wc}'>{d['미확보'] if d['미확보']>0 else '-'}</span>"
            "</div>"
        )

    st.markdown("**📂 계열 > 지점별 현황**")
    st.markdown(th(), unsafe_allow_html=True)
    ri = 0
    for ser, sv in s_tot.items():
        st.markdown(tr(ser, sv, "#ebf4ff", bold=True), unsafe_allow_html=True)
        for br, bv in s_br[ser].items():
            st.markdown(tr(br, bv, "#fff" if ri % 2 == 0 else "#f7fafc", indent=True),
                        unsafe_allow_html=True)
            ri += 1
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("**📄 과정 목록**")
    cf1, cf2, cf3 = st.columns(3)
    with cf1:
        sf = st.multiselect("진행상태", ["진행중","예정","완료","정보없음"],
                            default=["진행중","예정"])
    with cf2:
        vf = st.multiselect("강의장", ["확보","미확보","미확인"],
                            default=["확보","미확보","미확인"])
    with cf3:
        bf = st.multiselect("지점", sorted(set(c["지점"] for c in courses if c["지점"])))

    fc = [c for c in courses
          if c["진행상태"] in sf and c["강의장상태"] in vf
          and (not bf or c["지점"] in bf)]
    st.caption(f"{len(fc)}개 과정")

    if fc:
        st.dataframe(
            pd.DataFrame([{
                "계열": c["계열"], "지점": c["지점"], "훈련종류": c["훈련종류"],
                "과정명": c["과정명"], "시작일": c["시작일"], "종료일": c["종료일"],
                "정원": c["정원"], "진행상태": c["진행상태"], "강의장": c["강의장상태"],
            } for c in fc]),
            use_container_width=True, hide_index=True,
        )

# ══════════════════════════════════════════════
# TAB 1 : 모집현황 입력 (직접 입력 폼)
# ══════════════════════════════════════════════
with tab1:
    st.markdown("#### 주차별 모집현황 입력")

    if not sheet:
        st.warning(
            "Google Sheets가 연결되지 않아 저장된 데이터가 **브라우저 새로고침 시 초기화**됩니다.\n\n"
            "팀 공유를 위해 Google Sheets 설정을 완료해주세요.",
            icon="⚠️",
        )

    # ── 주차 / 계열 / 지점 선택 ──────────────────
    SERIES_OPTS = ["— 계열 선택 —", "IT", "컴퓨터", "게임", "뷰티", "요리", "승무원"]
    hdr1, hdr2, hdr3 = st.columns([1, 1, 2])
    with hdr1:
        week_label = st.text_input("기준 주차", placeholder="예: 3월3주",
                                   value=st.session_state.get("last_week",""))
    with hdr2:
        선택계열 = st.selectbox("계열 선택", SERIES_OPTS)
    with hdr3:
        입력지점 = st.text_input("지점명 입력", placeholder="예: 강남, 홍대, 신촌, 대구, 부산 등")

    if 선택계열 == "— 계열 선택 —" or not 입력지점.strip():
        st.info("계열을 선택하고 지점명을 입력하면 해당 과정 목록이 표시됩니다.")
    else:
        선택지점 = 입력지점.strip()
        # 계열과 지점으로 필터 (계열 부분 일치, 지점 부분 일치)
        지점과정 = [
            c for c in courses
            if 선택계열 in (c.get("계열") or "")
            and 선택지점 in (c.get("지점") or "")
        ]
        # 계열 일치 없으면 지점만으로 재시도
        if not 지점과정:
            지점과정 = [c for c in courses if 선택지점 in (c.get("지점") or "")]
        if not 지점과정:
            st.warning(f"'{선택지점}' 지점의 과정이 연간계획에 없습니다.")
        else:
            # 상태 아이콘 색 구분 설명
            st.caption(f"✅ 개강확정  🔄 개강연기  ✖ 폐강  |  총 {len(지점과정)}개 과정")
            st.markdown("---")

            STATE_OPTS = ["개강확정", "개강연기", "폐강"]
            입력결과 = []

            for i, c in enumerate(지점과정):
                key_c = course_key(c["지점"], c["과정명"], c.get("운영회차","1"))
                ex = db.get(key_c, {})

                # 현재 상태 아이콘
                cur_state = ex.get("개설상태", "개강확정")
                icon = "✅" if cur_state == "개강확정" else \
                       "🔄" if cur_state == "개강연기" else \
                       "✖" if cur_state == "폐강" else "⬜"

                date_str = ""
                if c.get("시작일") and c.get("종료일"):
                    try:
                        s = c["시작일"].strftime("%m/%d") if hasattr(c["시작일"], "strftime") else str(c["시작일"])[:5]
                        e = c["종료일"].strftime("%m/%d") if hasattr(c["종료일"], "strftime") else str(c["종료일"])[:5]
                        date_str = f" ({s}~{e})"
                    except Exception:
                        pass

                with st.expander(
                    f"{icon}  **{c['과정명']}**  —  정원 {c['정원']}명{date_str}",
                    expanded=(cur_state != "개강확정" or not ex)
                ):
                    c1, c2, c3 = st.columns([1, 1, 2])
                    with c1:
                        확정 = st.number_input(
                            "확정인원", min_value=0,
                            value=int(ex.get("확정인원", 0) or 0),
                            key=f"cf1_{i}"
                        )
                        신청 = st.number_input(
                            "신청인원", min_value=0,
                            value=int(ex.get("신청인원", 0) or 0),
                            key=f"ap1_{i}"
                        )
                    with c2:
                        정원 = c["정원"] or 1
                        모집률 = 확정 / 정원 * 100
                        신청률 = 신청 / 정원 * 100
                        mr_color = "red" if 모집률 < 65 else "green"
                        sr_color = "red" if 신청률 < 70 else "green"
                        st.markdown(
                            f"<br><span style='color:{mr_color};font-weight:700'>모집률 {모집률:.0f}%</span><br>"
                            f"<span style='color:{sr_color};font-weight:700'>신청률 {신청률:.0f}%</span>",
                            unsafe_allow_html=True
                        )
                    with c3:
                        state_idx = STATE_OPTS.index(cur_state) if cur_state in STATE_OPTS else 0
                        상태 = st.selectbox(
                            "개설상태", STATE_OPTS,
                            index=state_idx, key=f"st1_{i}"
                        )
                        사유 = st.text_input(
                            "연기/폐강 사유",
                            value=ex.get("연기사유", ""),
                            key=f"rs1_{i}",
                            placeholder="연기 또는 폐강 사유 입력"
                        )

                    비고 = st.text_input(
                        "비고", value=ex.get("모집비고", ""),
                        key=f"nt1_{i}", placeholder="기타 메모"
                    )

                    입력결과.append({
                        "course": c, "key": key_c,
                        "확정": 확정, "신청": 신청,
                        "모집률": round(확정 / 정원, 4) if 정원 > 0 else 0,
                        "신청률": round(신청 / 정원, 4) if 정원 > 0 else 0,
                        "상태": 상태, "사유": 사유, "비고": 비고,
                    })

            st.markdown("---")
            if st.button("💾 전체 저장", type="primary", use_container_width=True):
                if not week_label.strip():
                    st.error("기준 주차를 입력해주세요 (예: 3월3주)")
                else:
                    saved = 0
                    for item in 입력결과:
                        c = item["course"]
                        k = item["key"]
                        record = {
                            "key": k,
                            "계열": c["계열"], "지점": c["지점"],
                            "훈련종류": c["훈련종류"], "과정명": c["과정명"],
                            "시작일": str(c["시작일"]), "종료일": str(c["종료일"]),
                            "정원": c["정원"],
                            "기준주차": week_label.strip(),
                            "확정인원": item["확정"],
                            "신청인원": item["신청"],
                            "모집률": item["모집률"],
                            "신청률": item["신청률"],
                            "개설상태": item["상태"],
                            "연기사유": item["사유"],
                            "모집비고": item["비고"],
                            "평가완료":    db.get(k, {}).get("평가완료",""),
                            "평가완료일":  db.get(k, {}).get("평가완료일",""),
                            "평가비고":    db.get(k, {}).get("평가비고",""),
                            "비용신청":    db.get(k, {}).get("비용신청",""),
                            "비용금액":    db.get(k, {}).get("비용금액",""),
                            "비용신청일":  db.get(k, {}).get("비용신청일",""),
                            "비용비고":    db.get(k, {}).get("비용비고",""),
                            "취업_이수자": db.get(k, {}).get("취업_이수자",""),
                            "취업_취업자": db.get(k, {}).get("취업_취업자",""),
                            "취업_조사일": db.get(k, {}).get("취업_조사일",""),
                            "취업비고":    db.get(k, {}).get("취업비고",""),
                            "만족도점수":  db.get(k, {}).get("만족도점수",""),
                            "만족도조사일":db.get(k, {}).get("만족도조사일",""),
                            "만족도비고":  db.get(k, {}).get("만족도비고",""),
                            "업데이트": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        }
                        save_record(k, record)
                        if not sheet:
                            st.session_state.local_db[k] = record
                        db[k] = record
                        saved += 1
                    st.session_state["last_week"] = week_label.strip()
                    st.success(f"✅ {saved}개 과정 저장 완료! ({선택지점} / {week_label})")
                    st.rerun()

# ══════════════════════════════════════════════
# TAB 2 : 모집현황 조회
# ══════════════════════════════════════════════
with tab2:
    recs = [v for v in db.values() if v.get("개설상태","미입력") not in ("","미입력")]

    if not recs:
        st.info("저장된 모집현황이 없습니다. '모집현황 입력' 탭에서 먼저 데이터를 입력하세요.")
    else:
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1: sf2 = st.multiselect("계열", sorted(set(r.get("계열","") for r in recs)))
        with fc2: bf2 = st.multiselect("지점", sorted(set(r.get("지점","") for r in recs)))
        with fc3: stf = st.multiselect("개설상태", ["개강확정","개강연기","폐강"],
                                        default=["개강확정","개강연기","폐강"])
        with fc4: warn_only = st.checkbox("경고 과정만")

        filtered = [
            r for r in recs
            if (not sf2 or r.get("계열") in sf2)
            and (not bf2 or r.get("지점") in bf2)
            and r.get("개설상태") in stf
        ]
        if warn_only:
            filtered = [
                r for r in filtered
                if r.get("개설상태") == "개강확정"
                and (float(r.get("모집률",1) or 1) < 0.65
                     or float(r.get("신청률",1) or 1) < 0.70)
            ]

        # 요약 KPI
        개설 = [r for r in filtered if r.get("개설상태") == "개강확정"]
        경고 = [r for r in 개설
                if float(r.get("모집률",1) or 1) < 0.65
                or float(r.get("신청률",1) or 1) < 0.70]
        연기 = [r for r in filtered if r.get("개설상태") == "개강연기"]
        폐강 = [r for r in filtered if r.get("개설상태") == "폐강"]

        mc = st.columns(5)
        for col, num, lbl, clr in [
            (mc[0], len(filtered), "전체",    "#2b6cb0"),
            (mc[1], len(개설),     "개강확정", "#276749"),
            (mc[2], len(경고),     "모집경고", "#e53e3e"),
            (mc[3], len(연기),     "개강연기", "#c05621"),
            (mc[4], len(폐강),     "폐강",     "#718096"),
        ]:
            with col:
                st.markdown(
                    f'<div class="kpi-box"><div class="kpi-num" style="color:{clr};font-size:1.4rem">{num}</div>'
                    f'<div class="kpi-label">{lbl}</div></div>',
                    unsafe_allow_html=True,
                )
        st.markdown("<br>", unsafe_allow_html=True)

        # 테이블 (pandas styler)
        rows = []
        for r in filtered:
            mp = float(r.get("모집률", 0) or 0) * 100
            sp = float(r.get("신청률", 0) or 0) * 100
            rows.append({
                "계열":      r.get("계열",""),
                "지점":      r.get("지점",""),
                "훈련종류":  r.get("훈련종류",""),
                "과정명":    r.get("과정명",""),
                "기준주차":  r.get("기준주차",""),
                "정원":      int(r.get("정원",0) or 0),
                "확정":      int(r.get("확정인원",0) or 0),
                "신청":      int(r.get("신청인원",0) or 0),
                "모집률(%)": round(mp, 1),
                "신청률(%)": round(sp, 1),
                "개설상태":  r.get("개설상태",""),
                "연기사유":  r.get("연기사유",""),
                "비고":      r.get("모집비고",""),
            })

        df = pd.DataFrame(rows)

        def style_row(row):
            s = row["개설상태"]
            if s == "폐강":
                return ["color:#aaa;background:#f7fafc"] * len(row)
            if s == "개강연기":
                return ["background:#fffbeb"] * len(row)
            if s == "개강확정" and (row["모집률(%)"] < 65 or row["신청률(%)"] < 70):
                return ["background:#fff5f5;color:#c53030"] * len(row)
            return [""] * len(row)

        styled = (
            df.style
            .apply(style_row, axis=1)
            .map(lambda v: "color:#e53e3e;font-weight:700" if v < 65 else
                           "color:#276749;font-weight:700", subset=["모집률(%)"])
            .map(lambda v: "color:#e53e3e;font-weight:700" if v < 70 else
                           "color:#276749;font-weight:700", subset=["신청률(%)"])
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)

        # 경고 과정 하이라이트
        if 경고:
            st.markdown(f"**⚠️ 모집경고 과정 {len(경고)}건**")
            for r in 경고:
                mp = float(r.get("모집률",0) or 0) * 100
                sp = float(r.get("신청률",0) or 0) * 100
                st.error(
                    f"**{r.get('과정명','')}** ({r.get('지점','')})  |  "
                    f"확정 {r.get('확정인원',0)}/{r.get('정원',0)}명  |  "
                    f"모집률 **{mp:.1f}%**  신청률 **{sp:.1f}%**"
                    + (f"  |  비고: {r.get('모집비고','')}" if r.get("모집비고") else "")
                )

# ══════════════════════════════════════════════
# TAB 3 : 과정 추적 관리
# ══════════════════════════════════════════════
with tab3:
    st.markdown("#### 개강확정 과정별 추적 관리")
    st.caption("이수자 평가 · 비용신청 · 취업성과 · 만족도를 과정별로 입력·관리합니다.")

    confirmed_keys = [k for k, v in db.items() if v.get("개설상태") == "개강확정"]

    if not confirmed_keys:
        st.info("개강확정 과정이 없습니다. 모집현황 입력 탭에서 데이터를 저장하면 표시됩니다.")
    else:
        tfc1, tfc2 = st.columns(2)
        with tfc1:
            br_t = st.multiselect(
                "지점 필터",
                sorted(set(db[k].get("지점","") for k in confirmed_keys)),
                key="track_br",
            )
        with tfc2:
            view_item = st.selectbox(
                "입력 항목",
                ["전체보기","이수자평가","비용신청","취업성과","만족도"],
            )

        keys_show = [k for k in confirmed_keys
                     if not br_t or db[k].get("지점") in br_t]

        for key in keys_show:
            r  = db[key]
            mp = float(r.get("모집률",0) or 0) * 100
            label = (
                f"**{r.get('과정명','')}** — {r.get('지점','')} / {r.get('훈련종류','')}  |  "
                f"{r.get('시작일','')} ~ {r.get('종료일','')}  |  모집률 {mp:.1f}%"
            )
            with st.expander(label, expanded=False):
                with st.form(key=f"form_{key}"):
                    cols = st.columns(4)

                    # 이수자 평가
                    with cols[0]:
                        st.markdown("**📝 이수자 평가**")
                        ev_yn = st.checkbox("완료", value=str(r.get("평가완료","")) == "True",
                                            key=f"ev_{key}")
                        ev_dt = st.date_input("완료일",
                                              value=datetime.strptime(r["평가완료일"],"%Y-%m-%d").date()
                                              if r.get("평가완료일") else datetime.today(),
                                              key=f"evd_{key}")
                        ev_nt = st.text_input("비고", value=r.get("평가비고",""), key=f"evn_{key}")

                    # 비용 신청
                    with cols[1]:
                        st.markdown("**💰 비용 신청**")
                        cs_yn = st.checkbox("완료", value=str(r.get("비용신청","")) == "True",
                                            key=f"cs_{key}")
                        cs_am = st.number_input("금액(원)",
                                                value=int(r.get("비용금액",0) or 0),
                                                min_value=0, step=10000, key=f"csa_{key}")
                        cs_dt = st.date_input("신청일",
                                              value=datetime.strptime(r["비용신청일"],"%Y-%m-%d").date()
                                              if r.get("비용신청일") else datetime.today(),
                                              key=f"csd_{key}")
                        cs_nt = st.text_input("비고", value=r.get("비용비고",""), key=f"csn_{key}")

                    # 취업 성과
                    with cols[2]:
                        st.markdown("**💼 취업 성과**")
                        em_total = st.number_input("이수자수",
                                                   value=int(r.get("취업_이수자",0) or 0),
                                                   min_value=0, key=f"emt_{key}")
                        em_hired = st.number_input("취업자수",
                                                   value=int(r.get("취업_취업자",0) or 0),
                                                   min_value=0, key=f"emh_{key}")
                        em_dt = st.date_input("조사일",
                                              value=datetime.strptime(r["취업_조사일"],"%Y-%m-%d").date()
                                              if r.get("취업_조사일") else datetime.today(),
                                              key=f"emd_{key}")
                        em_nt = st.text_input("비고", value=r.get("취업비고",""), key=f"emn_{key}")
                        if em_total > 0:
                            st.metric("취업률", f"{em_hired/em_total*100:.1f}%")

                    # 만족도
                    with cols[3]:
                        st.markdown("**⭐ 만족도**")
                        sat_sc = st.number_input("점수 (0~5)",
                                                 value=float(r.get("만족도점수",0) or 0),
                                                 min_value=0.0, max_value=5.0, step=0.1,
                                                 key=f"sas_{key}")
                        sat_dt = st.date_input("조사일",
                                               value=datetime.strptime(r["만족도조사일"],"%Y-%m-%d").date()
                                               if r.get("만족도조사일") else datetime.today(),
                                               key=f"sad_{key}")
                        sat_nt = st.text_input("비고", value=r.get("만족도비고",""), key=f"san_{key}")

                    if st.form_submit_button("💾 저장", use_container_width=True):
                        updated = dict(r)
                        updated.update({
                            "평가완료": str(ev_yn), "평가완료일": ev_dt.strftime("%Y-%m-%d"),
                            "평가비고": ev_nt,
                            "비용신청": str(cs_yn), "비용금액": cs_am,
                            "비용신청일": cs_dt.strftime("%Y-%m-%d"), "비용비고": cs_nt,
                            "취업_이수자": em_total, "취업_취업자": em_hired,
                            "취업_조사일": em_dt.strftime("%Y-%m-%d"), "취업비고": em_nt,
                            "만족도점수": sat_sc,
                            "만족도조사일": sat_dt.strftime("%Y-%m-%d"), "만족도비고": sat_nt,
                            "업데이트": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        })
                        save_record(key, updated)
                        if not sheet:
                            st.session_state.local_db[key] = updated
                        db[key] = updated
                        st.success("저장 완료!")
                        st.rerun()

        # 집계
        st.markdown("---")
        st.markdown("**📊 추적 항목 집계**")
        agg = st.columns(4)
        ev_done  = sum(1 for k in confirmed_keys
                       if str(db[k].get("평가완료","")) == "True")
        cs_done  = sum(1 for k in confirmed_keys
                       if str(db[k].get("비용신청","")) == "True")
        em_list  = [(int(db[k].get("취업_이수자",0) or 0),
                     int(db[k].get("취업_취업자",0) or 0))
                    for k in confirmed_keys
                    if int(db[k].get("취업_이수자",0) or 0) > 0]
        avg_emp  = (sum(h/t for t,h in em_list)/len(em_list)*100) if em_list else 0
        sat_list = [float(db[k].get("만족도점수",0) or 0)
                    for k in confirmed_keys
                    if float(db[k].get("만족도점수",0) or 0) > 0]
        avg_sat  = sum(sat_list)/len(sat_list) if sat_list else 0

        for col, num, lbl, clr in [
            (agg[0], f"{ev_done}/{len(confirmed_keys)}", "이수자평가 완료", "#2b6cb0"),
            (agg[1], f"{cs_done}/{len(confirmed_keys)}", "비용신청 완료",   "#276749"),
            (agg[2], f"{avg_emp:.1f}%",                  "평균 취업률",     "#553c9a"),
            (agg[3], f"{avg_sat:.2f}/5.0",               "평균 만족도",     "#744210"),
        ]:
            with col:
                st.markdown(
                    f'<div class="kpi-box"><div class="kpi-num" style="color:{clr};font-size:1.35rem">{num}</div>'
                    f'<div class="kpi-label">{lbl}</div></div>',
                    unsafe_allow_html=True,
                )

# ══════════════════════════════════════════════
# TAB 4 : 반납 분석
# ══════════════════════════════════════════════
with tab4:
    from collections import Counter
    rc = Counter(r["사유분류"] for r in returns)

    if not rc:
        st.info("반납 과정이 없습니다.")
    else:
        COLORS = {
            "강의장 부족":"#FED7D7","모집률 저조":"#FEFCBF","미개설":"#EDF2F7",
            "강사 문제":"#BEE3F8","비용/효율성":"#C6F6D5","직종 중복/조정":"#E9D8FD",
            "내부 결정":"#FBD38D","기타":"#EDF2F7",
        }
        TC = {
            "강의장 부족":"#822727","모집률 저조":"#744210","미개설":"#4a5568",
            "강사 문제":"#2a4365","비용/효율성":"#276749","직종 중복/조정":"#553c9a",
            "내부 결정":"#7b341e","기타":"#4a5568",
        }
        rcols = st.columns(min(len(rc), 4))
        for i, (reason, cnt) in enumerate(sorted(rc.items(), key=lambda x: -x[1])):
            bg = COLORS.get(reason,"#EDF2F7"); tc2 = TC.get(reason,"#4a5568")
            with rcols[i % 4]:
                st.markdown(
                    f"<div style='background:{bg};border-radius:8px;padding:0.8rem;"
                    f"text-align:center;margin-bottom:0.5rem'>"
                    f"<div style='font-size:1.5rem;font-weight:700;color:{tc2}'>{cnt}건</div>"
                    f"<div style='font-size:0.8rem;color:{tc2};font-weight:600'>{reason}</div></div>",
                    unsafe_allow_html=True,
                )

        sel = st.selectbox("사유 필터", ["전체"] + list(rc.keys()))
        show = returns if sel == "전체" else [r for r in returns if r["사유분류"] == sel]
        st.dataframe(
            pd.DataFrame([{
                "계열":r["계열"],"지점":r["지점"],"훈련종류":r["훈련종류"],
                "과정명":r["과정명"],"사유분류":r["사유분류"],"반납사유":r["반납사유"],
            } for r in show]),
            use_container_width=True, hide_index=True,
        )
