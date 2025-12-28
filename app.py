# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v6.3 (FULL FILE, RTL, Mobile/Tablet friendly)

What you get:
1) Upload Excel → choose Agent.
2) סעיף 2 "דוח מסכם — דוח יעדים לסוכן" is NOT shown as a table on screen.
   It is available ONLY as an Excel download button.
   Excel includes: שם לקוח | מכירות 2025 | יעד 2026 | תוספת בכסף | תוספת באחוזים + שורת סה"כ בתחתית.
3) Customers selector = multiselect (default NONE).
   - None selected → right side shows ALL agent scope (combined classes + KPI).
   - Single customer → editable class targets + KPI shows customer share% next to 2026 target.
   - Multiple customers → read-only scope (selected customers).
4) Single customer export (styled) — green highlight only where "תוספת יעד (כמות)" entered.

Run:
  streamlit run app.py
"""

import math
import re
import sqlite3
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

# =========================
# Page Config + Theme
# =========================
st.set_page_config(page_title="Uzeb — Targets", layout="wide")

st.markdown(
    """
<style>
html, body, [class*="css"] { direction: rtl; font-family: "Heebo","Segoe UI",system-ui,sans-serif; }
.block-container { padding-top: 1.0rem; padding-bottom: 2rem; }
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }

.card {
  background: rgba(255,255,255,0.92);
  border: 1px solid rgba(0,0,0,0.08);
  border-radius: 18px;
  padding: 14px 16px;
  box-shadow: 0 10px 24px rgba(0,0,0,0.06);
  margin-bottom: 14px;
}
.card h1, .card h2, .card h3 { margin: 0 0 6px 0; font-weight: 900; }
.card p { margin: 0; opacity: 0.82; }

.kpi-grid { display:flex; gap:12px; flex-wrap:wrap; margin: 8px 0 12px 0; }
.kpi {
  background: rgba(255,255,255,0.92);
  border: 1px solid rgba(0,0,0,0.08);
  border-radius: 16px;
  padding: 12px 14px;
  min-width: 220px;
  box-shadow: 0 8px 18px rgba(0,0,0,0.05);
}
.kpi .label { font-size: 0.82rem; opacity: 0.70; }
.kpi .value { font-size: 1.45rem; font-weight: 900; margin-top: 2px; }
.kpi .sub   { font-size: 0.80rem; opacity: 0.72; margin-top: 2px; }

div.stButton > button { border-radius: 12px !important; font-weight: 900 !important; }
div.stButton > button.kg-rerun {
  background: #16a34a !important;
  color: white !important;
  border: 1px solid rgba(0,0,0,0.12) !important;
}
div.stButton > button.kg-rerun:hover { filter: brightness(0.97); }

[data-testid="stDataFrame"], [data-testid="stTable"] { border-radius: 12px; overflow: hidden; }

/* Mobile/Tablet */
@media (max-width: 900px) {
  .block-container { padding-left: 0.75rem !important; padding-right: 0.75rem !important; }
  .card { padding: 12px 12px; border-radius: 16px; }
  .kpi { min-width: 160px; flex: 1 1 160px; }
  .kpi .value { font-size: 1.25rem; }
}
@media (max-width: 768px) {
  div[data-testid="stHorizontalBlock"] { flex-direction: column !important; }
  div[data-testid="column"] { width: 100% !important; flex: 1 1 100% !important; }
  .kpi { min-width: 100%; }
  div.stButton > button { width: 100% !important; }
}
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# Excel Columns
# =========================
COL_AGENT = "סוכן בחשבון"
COL_ACCOUNT = "שם חשבון"
COL_CLASS = "שם קוד מיון פריט"
COL_ITEM = "שם פריט"  # optional
COL_QTY = "סהכ כמות"
COL_NET = "מכירות/קניות נטו"

# =========================
# Agent mapping
# =========================
AGENT_NAME_MAP = {"2": "אופיר", "15": "אנדי", "4": "ציקו", "7": "זוהר", "1": "משרד"}


def agent_label(agent_raw) -> str:
    a = str(agent_raw).strip()
    name = AGENT_NAME_MAP.get(a)
    return f"{a} — {name}" if name else a


# =========================
# DB (deploy-safe)
# =========================
DB_FILENAME = "uzeb_targets.sqlite"
DEFAULT_DB_DIR = Path(".") / "data"

if "db_dir" not in st.session_state:
    st.session_state["db_dir"] = str(DEFAULT_DB_DIR)


def get_db_path() -> Path:
    d = Path(str(st.session_state.get("db_dir", str(DEFAULT_DB_DIR))).strip())
    return d / DB_FILENAME


def ensure_db_dir_exists(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)


def db_connect(db_path: Path):
    ensure_db_dir_exists(db_path)
    con = sqlite3.connect(db_path.as_posix(), check_same_thread=False)

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS class_delta_qty (
            agent TEXT NOT NULL,
            account TEXT NOT NULL,
            cls TEXT NOT NULL,
            delta_qty REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (agent, account, cls)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS class_delta_money (
            agent TEXT NOT NULL,
            account TEXT NOT NULL,
            cls TEXT NOT NULL,
            delta_money REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (agent, account, cls)
        )
        """
    )
    con.commit()
    return con


@st.cache_resource
def get_db(db_path_str: str):
    return db_connect(Path(db_path_str))


def db_load_all_qty(con) -> dict:
    rows = con.execute("SELECT agent, account, cls, delta_qty FROM class_delta_qty").fetchall()
    return {(str(ag), str(acc), str(cls)): float(dq or 0.0) for ag, acc, cls, dq in rows}


def db_load_all_money(con) -> dict:
    rows = con.execute("SELECT agent, account, cls, delta_money FROM class_delta_money").fetchall()
    return {(str(ag), str(acc), str(cls)): float(dm or 0.0) for ag, acc, cls, dm in rows}


def db_upsert_qty(con, agent: str, account: str, cls: str, delta_qty: float):
    now = datetime.now(timezone.utc).isoformat()
    con.execute(
        """
        INSERT INTO class_delta_qty(agent, account, cls, delta_qty, updated_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(agent, account, cls) DO UPDATE SET
            delta_qty=excluded.delta_qty,
            updated_at=excluded.updated_at
        """,
        (str(agent), str(account), str(cls), float(delta_qty or 0.0), now),
    )
    con.commit()


# =========================
# Helpers
# =========================
def safe_div(a, b):
    if b in (0, 0.0) or pd.isna(b):
        return math.nan
    return a / b


def fmt_money(x) -> str:
    try:
        return f"₪ {float(x):,.2f}"
    except Exception:
        return "₪ 0.00"


def fmt_pct(x) -> str:
    if pd.isna(x):
        return "—"
    return f"{float(x):,.1f}%"


def safe_filename(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r'[\\/:*?"<>|]+', "_", s)
    s = re.sub(r"\s+", " ", s)
    return s[:60] if len(s) > 60 else s


def detect_header_row(file_like, needle=COL_AGENT, max_rows=25) -> int:
    preview = pd.read_excel(file_like, header=None, nrows=max_rows)
    for r in range(preview.shape[0]):
        vals = [str(x).strip() for x in preview.iloc[r].tolist()]
        if needle in vals:
            return r
    return 0


def read_sales_excel(uploaded_file) -> pd.DataFrame:
    raw = uploaded_file.getvalue()
    bio = BytesIO(raw)
    header_row = detect_header_row(bio)
    bio.seek(0)
    return pd.read_excel(bio, header=header_row)


def normalize_sales(df: pd.DataFrame) -> pd.DataFrame:
    required = {COL_AGENT, COL_ACCOUNT, COL_CLASS, COL_QTY, COL_NET}
    missing = [c for c in required if c not in df.columns]
    if missing:
        st.error(f"חסרות עמודות בקובץ: {missing}")
        st.stop()

    out = df.copy()
    out = out[out[COL_ACCOUNT].notna()]

    out[COL_AGENT] = out[COL_AGENT].astype(str).str.strip()
    out[COL_ACCOUNT] = out[COL_ACCOUNT].astype(str).str.strip()
    out[COL_CLASS] = out[COL_CLASS].astype(str).str.strip()

    if COL_ITEM in out.columns:
        out[COL_ITEM] = out[COL_ITEM].astype(str).str.strip()

    out[COL_QTY] = pd.to_numeric(out[COL_QTY], errors="coerce").fillna(0.0)
    out[COL_NET] = pd.to_numeric(out[COL_NET], errors="coerce").fillna(0.0)
    return out


def compute_classes(df: pd.DataFrame) -> pd.DataFrame:
    g = (
        df.groupby(COL_CLASS, dropna=False)
        .agg(מכירות_בכסף=(COL_NET, "sum"), מכירות_בכמות=(COL_QTY, "sum"))
        .reset_index()
        .sort_values("מכירות_בכסף", ascending=False)
        .reset_index(drop=True)
    )
    g["מחיר_ממוצע"] = g.apply(lambda r: safe_div(r["מכירות_בכסף"], r["מכירות_בכמות"]), axis=1)
    return g


def kpi_block(display_sales_2026: float, base_sales_2025: float, added_money: float, growth_pct: float, share_pct: float | None):
    share_line = ""
    if share_pct is not None and not pd.isna(share_pct):
        share_line = f"<div class='sub'>נתח לקוח מהמכירות של הסוכן: {fmt_pct(share_pct)}</div>"

    st.markdown(
        f"""
        <div class="kpi-grid">
            <div class="kpi">
                <div class="label">יעד 2026 (₪)</div>
                <div class="value">{fmt_money(display_sales_2026)}</div>
                {share_line}
            </div>
            <div class="kpi">
                <div class="label">מכירות 2025 (₪)</div>
                <div class="value">{fmt_money(base_sales_2025)}</div>
                <div class="sub">סכום נטו מהקובץ</div>
            </div>
            <div class="kpi">
                <div class="label">תוספת בכסף (₪)</div>
                <div class="value">{fmt_money(added_money)}</div>
                <div class="sub">2026 - 2025</div>
            </div>
            <div class="kpi">
                <div class="label">תוספת מכירות באחוזים (%)</div>
                <div class="value">{fmt_pct(growth_pct)}</div>
                <div class="sub">(2026/2025)*100 - 100</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =========================
# Targets logic (qty-driven)
# =========================
def get_delta_qty_for_row(qty_dict: dict, money_dict: dict, agent: str, account: str, cls: str, avg_price: float) -> float:
    key = (str(agent), str(account), str(cls))
    if key in qty_dict:
        return float(qty_dict.get(key, 0.0) or 0.0)

    dm = float(money_dict.get(key, 0.0) or 0.0)
    if dm == 0.0 or pd.isna(avg_price) or float(avg_price) == 0:
        return 0.0
    return float(dm) / float(avg_price)


def build_class_view(qty_dict: dict, money_dict: dict, agent: str, account: str, df_customer: pd.DataFrame) -> pd.DataFrame:
    class_df = compute_classes(df_customer)

    class_df["תוספת_יעד_כמות"] = class_df.apply(
        lambda r: get_delta_qty_for_row(
            qty_dict=qty_dict,
            money_dict=money_dict,
            agent=agent,
            account=account,
            cls=str(r[COL_CLASS]),
            avg_price=r["מחיר_ממוצע"],
        ),
        axis=1,
    )

    def qty_to_money(r):
        p = r["מחיר_ממוצע"]
        dq = float(r["תוספת_יעד_כמות"] or 0.0)
        if pd.isna(p) or float(p) == 0:
            return math.nan
        return dq * float(p)

    class_df["תוספת_יעד_כסף"] = class_df.apply(qty_to_money, axis=1)
    class_df["יעד_בכמות"] = class_df["מכירות_בכמות"] + class_df["תוספת_יעד_כמות"]

    def final_money(r):
        sales_m = float(r["מכירות_בכסף"] or 0.0)
        add_m = r["תוספת_יעד_כסף"]
        if pd.isna(add_m):
            return sales_m
        return sales_m + float(add_m)

    class_df["יעד_בכסף"] = class_df.apply(final_money, axis=1)
    class_df["פער_כמות"] = class_df["יעד_בכמות"] - class_df["מכירות_בכמות"]
    class_df["% עמידה"] = class_df.apply(
        lambda r: (r["מכירות_בכסף"] / r["יעד_בכסף"] * 100) if float(r["יעד_בכסף"] or 0) > 0 else math.nan,
        axis=1,
    )

    out = class_df[
        [
            COL_CLASS,
            "מכירות_בכסף",
            "מכירות_בכמות",
            "מחיר_ממוצע",
            "תוספת_יעד_כסף",
            "תוספת_יעד_כמות",
            "יעד_בכסף",
            "יעד_בכמות",
            "פער_כמות",
            "% עמידה",
        ]
    ].copy()
    out = out.rename(columns={COL_CLASS: "שם קוד מיון פריט"})
    return out


# =========================
# Agent Summary (df + Excel) — download only (no on-screen table)
# =========================
def build_agent_summary_report(agent_raw: str, agent_df: pd.DataFrame, delta_qty_dict: dict, delta_money_dict: dict) -> pd.DataFrame:
    customers = agent_df[COL_ACCOUNT].dropna().astype(str).unique().tolist()
    rows = []
    for acc in customers:
        df_c = agent_df[agent_df[COL_ACCOUNT].astype(str) == str(acc)].copy()
        if df_c.empty:
            continue

        class_view = build_class_view(delta_qty_dict, delta_money_dict, agent_raw, str(acc), df_c)
        s2025 = float(pd.to_numeric(class_view["מכירות_בכסף"], errors="coerce").fillna(0.0).sum())
        add_money = float(pd.to_numeric(class_view["תוספת_יעד_כסף"], errors="coerce").fillna(0.0).sum())
        s2026 = s2025 + add_money
        add_pct = (safe_div(s2026, s2025) * 100 - 100) if s2025 > 0 else math.nan

        rows.append(
            {
                "שם לקוח": str(acc),
                "מכירות בכסף 2025": s2025,
                "יעד מכירות 2026": s2026,
                "תוספת בכסף": add_money,
                "תוספת מכירות באחוזים": add_pct,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values("מכירות בכסף 2025", ascending=False).reset_index(drop=True)

    t2025 = float(pd.to_numeric(df["מכירות בכסף 2025"], errors="coerce").fillna(0.0).sum())
    t2026 = float(pd.to_numeric(df["יעד מכירות 2026"], errors="coerce").fillna(0.0).sum())
    tadd = float(pd.to_numeric(df["תוספת בכסף"], errors="coerce").fillna(0.0).sum())
    tpct = (safe_div(t2026, t2025) * 100 - 100) if t2025 > 0 else math.nan

    df_total = pd.DataFrame(
        [
            {
                "שם לקוח": "סה״כ",
                "מכירות בכסף 2025": t2025,
                "יעד מכירות 2026": t2026,
                "תוספת בכסף": tadd,
                "תוספת מכירות באחוזים": tpct,
            }
        ]
    )
    return pd.concat([df, df_total], ignore_index=True)


def make_agent_summary_excel(agent_display: str, df_summary: pd.DataFrame) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "דוח מסכם"
    ws.sheet_view.rightToLeft = True

    font_title = Font(bold=True, size=13)
    font_bold = Font(bold=True)
    align_center = Alignment(horizontal="center", vertical="center")
    align_right = Alignment(horizontal="right", vertical="center")
    thin = Side(style="thin", color="D0D0D0")
    border_all = Border(left=thin, right=thin, top=thin, bottom=thin)
    fill_header = PatternFill("solid", fgColor="F3F4F6")
    fill_total = PatternFill("solid", fgColor="E5E7EB")

    ws.merge_cells("A1:E1")
    ws["A1"].value = f"דוח יעדים לסוכן — דוח מסכם: {agent_display}"
    ws["A1"].font = font_title
    ws["A1"].alignment = align_right
    ws.row_dimensions[1].height = 22

    start_row = 3
    cols = [
        "שם לקוח",
        "מכירות בכסף 2025",
        "יעד מכירות 2026",
        "תוספת בכסף",
        "תוספת מכירות באחוזים",
    ]

    df = df_summary.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    for j, col_name in enumerate(cols, start=1):
        cell = ws.cell(row=start_row, column=j, value=col_name)
        cell.font = font_bold
        cell.fill = fill_header
        cell.alignment = align_center
        cell.border = border_all

    data_start = start_row + 1
    for i, row in enumerate(df.itertuples(index=False), start=data_start):
        is_total = (str(row[0]).strip() == "סה״כ")
        for j, value in enumerate(row, start=1):
            c = ws.cell(row=i, column=j, value=value)
            c.border = border_all
            c.alignment = align_right if j == 1 else align_center
            if j in (2, 3, 4):
                c.number_format = "#,##0.00"
            elif j == 5:
                c.number_format = "0.0"
            if is_total:
                c.font = font_bold
                c.fill = fill_total

    widths = {1: 34, 2: 18, 3: 18, 4: 16, 5: 18}
    for j, w in widths.items():
        ws.column_dimensions[get_column_letter(j)].width = w

    ws.freeze_panes = ws["A4"]

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


# =========================
# Export: Single customer (styled, green only when target entered)
# =========================
def make_styled_export_excel(agent_display: str, account_display: str, df_classes: pd.DataFrame) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Classes"
    ws.sheet_view.rightToLeft = True

    font_bold = Font(bold=True)
    font_title = Font(bold=True, size=12)
    align_center = Alignment(horizontal="center", vertical="center")
    align_right = Alignment(horizontal="right", vertical="center")
    thin = Side(style="thin", color="D0D0D0")
    border_all = Border(left=thin, right=thin, top=thin, bottom=thin)

    fill_header = PatternFill("solid", fgColor="F3F4F6")
    fill_green_soft = PatternFill("solid", fgColor="86EFAC")

    ws.merge_cells("A1:C1")
    ws.merge_cells("D1:F1")
    ws["A1"].value = f"סוכן: {agent_display}"
    ws["D1"].value = f"לקוח: {account_display}"
    ws["A1"].font = font_title
    ws["D1"].font = font_title
    ws["A1"].alignment = align_right
    ws["D1"].alignment = align_right
    ws.row_dimensions[1].height = 22
    ws.row_dimensions[2].height = 10

    start_row = 3
    start_col = 1

    cols = [
        "שם קוד מיון פריט",
        "מכירות_בכסף",
        "מכירות_בכמות",
        "מחיר_ממוצע",
        "תוספת_יעד_כסף",
        "תוספת_יעד_כמות",
        "יעד_בכסף",
        "יעד_בכמות",
        "פער_כמות",
        "% עמידה",
    ]

    df = df_classes.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    for j, col_name in enumerate(cols, start=start_col):
        cell = ws.cell(row=start_row, column=j, value=col_name)
        cell.font = font_bold
        cell.fill = fill_header
        cell.alignment = align_center
        cell.border = border_all

    data_start = start_row + 1
    for i, row in enumerate(df.itertuples(index=False), start=data_start):
        for j, value in enumerate(row, start=start_col):
            c = ws.cell(row=i, column=j, value=value)
            c.alignment = align_right if j == start_col else align_center
            c.border = border_all

            header = cols[j - start_col]
            if header in ("מכירות_בכסף", "מחיר_ממוצע", "תוספת_יעד_כסף", "יעד_בכסף"):
                c.number_format = "#,##0.00"
            elif header in ("מכירות_בכמות", "תוספת_יעד_כמות", "יעד_בכמות", "פער_כמות"):
                c.number_format = "#,##0.00"
            elif header == "% עמידה":
                c.number_format = "0.0"

        dq_col_idx = cols.index("תוספת_יעד_כמות") + start_col
        dm_col_idx = cols.index("תוספת_יעד_כסף") + start_col
        dq_cell = ws.cell(row=i, column=dq_col_idx)
        dm_cell = ws.cell(row=i, column=dm_col_idx)

        try:
            dq_val = float(dq_cell.value) if dq_cell.value is not None else 0.0
            if abs(dq_val) > 0:
                dm_cell.fill = fill_green_soft
                dm_cell.font = Font(bold=True)
        except Exception:
            pass

    widths = {"A": 34, "B": 14, "C": 14, "D": 14, "E": 16, "F": 16, "G": 14, "H": 14, "I": 14, "J": 12}
    for col_letter, w in widths.items():
        ws.column_dimensions[col_letter].width = w

    ws.freeze_panes = ws["A4"]

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


# =========================
# UI Header
# =========================
st.markdown(
    """
<div class="card">
  <h2>📊 Uzeb — ניהול יעדי מכירות</h2>
  <p>העלה קובץ → בחר סוכן → (אופציונלי) בחר לקוחות → צפה/ערוך יעדים → הורד דוחות.</p>
</div>
""",
    unsafe_allow_html=True,
)

# =========================
# Sidebar
# =========================
with st.sidebar:
    st.markdown("### שלבים")
    st.caption("1) העלה קובץ  →  2) בחר סוכן  →  3) הורד דוח מסכם  →  4) בחר לקוחות (אופציונלי)")

    rerun_clicked = st.button("רענון", use_container_width=True)
    st.markdown(
        """
        <script>
        const btns = window.parent.document.querySelectorAll('button');
        for (const b of btns) { if (b.innerText.trim() === 'רענון') { b.classList.add('kg-rerun'); } }
        </script>
        """,
        unsafe_allow_html=True,
    )
    if rerun_clicked:
        st.rerun()

    st.markdown("---")
    st.markdown("### העלאת קובץ")
    uploaded = st.file_uploader("Excel (.xlsx)", type=["xlsx"], accept_multiple_files=False)

    st.markdown("---")
    st.markdown("### שמירה (SQLite)")
    st.text_input("נתיב תיקייה למסד נתונים", key="db_dir")
    st.caption(f"DB: {get_db_path().as_posix()}")

# =========================
# DB init / load
# =========================
db_path = get_db_path()
con = get_db(str(db_path))

if (
    "delta_qty_dict" not in st.session_state
    or "delta_money_dict" not in st.session_state
    or st.session_state.get("db_path_last") != str(db_path)
):
    st.session_state["delta_qty_dict"] = db_load_all_qty(con)
    st.session_state["delta_money_dict"] = db_load_all_money(con)
    st.session_state["db_path_last"] = str(db_path)

delta_qty_dict = st.session_state["delta_qty_dict"]
delta_money_dict = st.session_state["delta_money_dict"]

# =========================
# Stop early
# =========================
if uploaded is None:
    st.info("⬅️ העלה קובץ Excel מהצד כדי להתחיל.")
    st.stop()

# =========================
# Load & normalize
# =========================
with st.spinner("טוען קובץ ומחשב נתונים..."):
    sales = normalize_sales(read_sales_excel(uploaded))

# =========================
# Choose agent
# =========================
st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown("### 1) בחירת סוכן")
agents_raw = sorted(sales[COL_AGENT].unique().tolist(), key=lambda x: str(x))
selected_agent = st.selectbox("בחר סוכן", agents_raw, format_func=agent_label)
st.markdown("</div>", unsafe_allow_html=True)

agent_df = sales[sales[COL_AGENT].astype(str) == str(selected_agent)].copy()
agent_total_money_2025 = float(agent_df[COL_NET].sum())

# =========================
# סעיף 2: דוח מסכם — Download ONLY (no table on screen)
# =========================
summary_df = build_agent_summary_report(
    agent_raw=str(selected_agent),
    agent_df=agent_df,
    delta_qty_dict=delta_qty_dict,
    delta_money_dict=delta_money_dict,
)

st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown(f"### 2) דוח מסכם — דוח יעדים לסוכן: {agent_label(selected_agent)}")
st.caption("הדוח זמין רק להורדה כ-Excel. כולל שורת סה״כ בתחתית.")

summary_filename = f"uzeb_{safe_filename(str(selected_agent))}__agent_summary__2026.xlsx"
summary_xls = make_agent_summary_excel(agent_label(selected_agent), summary_df)

st.download_button(
    "⬇️ הורד דוח מסכם (Excel)",
    data=summary_xls,
    file_name=summary_filename,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True,
)
st.markdown("</div>", unsafe_allow_html=True)

# =========================
# Customers summary table (agent-wide)
# =========================
cust_table = (
    agent_df.groupby(COL_ACCOUNT)
    .agg(סהכ_כסף=(COL_NET, "sum"), סהכ_כמות=(COL_QTY, "sum"))
    .reset_index()
    .sort_values("סהכ_כסף", ascending=False)
    .reset_index(drop=True)
)
cust_table["נתח_מכירות_מהסוכן (%)"] = cust_table["סהכ_כסף"].apply(
    lambda x: safe_div(float(x), agent_total_money_2025) * 100 if agent_total_money_2025 > 0 else math.nan
)

customer_options = cust_table[COL_ACCOUNT].astype(str).tolist()

# =========================
# Selection state — DEFAULT = NONE
# =========================
sel_key = f"cust_selection::{selected_agent}"
if sel_key not in st.session_state:
    st.session_state[sel_key] = []

left, right = st.columns([1, 2], gap="large")

with left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### 3) בחירת לקוחות (אופציונלי)")
    st.caption("ברירת מחדל: לא נבחר לקוח → תצוגה מלאה לסוכן. לקוח יחיד → עריכה. מספר לקוחות → תצוגה מסוננת לקריאה בלבד.")

    selected_customers = st.multiselect(
        "לקוחות (מסודר לפי מכירות)",
        options=customer_options,
        default=st.session_state.get(sel_key, []),
    )
    st.session_state[sel_key] = selected_customers

    st.markdown("#### טבלת לקוחות — סוכן (2025)")
    st.dataframe(
        cust_table[[COL_ACCOUNT, "סהכ_כסף", "סהכ_כמות", "נתח_מכירות_מהסוכן (%)"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            COL_ACCOUNT: st.column_config.TextColumn("שם לקוח"),
            "סהכ_כסף": st.column_config.NumberColumn("מכירות 2025 (₪)", format="%.2f"),
            "סהכ_כמות": st.column_config.NumberColumn("כמות", format="%.2f"),
            "נתח_מכירות_מהסוכן (%)": st.column_config.NumberColumn("נתח מהסוכן (%)", format="%.1f"),
        },
    )
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    selected_customers = [str(x) for x in st.session_state.get(sel_key, [])]
    none_selected = len(selected_customers) == 0
    single = len(selected_customers) == 1

    if none_selected:
        df_scope = agent_df.copy()
        scope_title = "כל המכירות של הסוכן"
        share_pct = None
    elif single:
        df_scope = agent_df[agent_df[COL_ACCOUNT].astype(str) == str(selected_customers[0])].copy()
        scope_title = f"לקוח: {selected_customers[0]}"
        cust_sales_2025 = float(df_scope[COL_NET].sum())
        share_pct = safe_div(cust_sales_2025, agent_total_money_2025) * 100 if agent_total_money_2025 > 0 else math.nan
    else:
        df_scope = agent_df[agent_df[COL_ACCOUNT].astype(str).isin(selected_customers)].copy()
        scope_title = f"{len(selected_customers)} לקוחות (מסונן)"
        share_pct = None

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### 4) תצוגה")
    st.caption(f"סוכן: {agent_label(selected_agent)} | תצוגה: {scope_title}")
    st.markdown("</div>", unsafe_allow_html=True)

    with st.spinner("מחשב תצוגה..."):
        class_sales = compute_classes(df_scope).rename(columns={COL_CLASS: "שם קוד מיון פריט"})

        def agg_qty_delta(cls: str) -> float:
            total = 0.0
            for (ag, acc, c), dq in delta_qty_dict.items():
                if str(ag) != str(selected_agent):
                    continue
                if none_selected:
                    pass
                else:
                    if str(acc) not in selected_customers:
                        continue
                if str(c) == str(cls):
                    total += float(dq or 0.0)
            return total

        class_sales["תוספת_יעד_כמות"] = class_sales["שם קוד מיון פריט"].astype(str).apply(agg_qty_delta)

        def qty_to_money(r):
            p = r["מחיר_ממוצע"]
            dq = float(r["תוספת_יעד_כמות"] or 0.0)
            if pd.isna(p) or float(p) == 0:
                return math.nan
            return dq * float(p)

        class_sales["תוספת_יעד_כסף"] = class_sales.apply(qty_to_money, axis=1)
        class_sales["יעד_בכמות"] = class_sales["מכירות_בכמות"] + class_sales["תוספת_יעד_כמות"]
        class_sales["יעד_בכסף"] = class_sales.apply(
            lambda r: float(r["מכירות_בכסף"] or 0.0) + (0.0 if pd.isna(r["תוספת_יעד_כסף"]) else float(r["תוספת_יעד_כסף"])),
            axis=1,
        )
        class_sales["פער_כמות"] = class_sales["יעד_בכמות"] - class_sales["מכירות_בכמות"]
        class_sales["% עמידה"] = class_sales.apply(
            lambda r: (r["מכירות_בכסף"] / r["יעד_בכסף"] * 100) if float(r["יעד_בכסף"] or 0) > 0 else math.nan,
            axis=1,
        )

    base_sales_2025 = float(pd.to_numeric(class_sales["מכירות_בכסף"], errors="coerce").fillna(0.0).sum())
    added_money = float(pd.to_numeric(class_sales["תוספת_יעד_כסף"], errors="coerce").fillna(0.0).sum())
    sales_2026 = base_sales_2025 + added_money
    growth_pct = (safe_div(sales_2026, base_sales_2025) * 100 - 100) if base_sales_2025 > 0 else math.nan

    kpi_block(sales_2026, base_sales_2025, added_money, growth_pct, share_pct if single else None)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("#### טבלת קודי מיון")
    st.dataframe(
        class_sales[
            [
                "שם קוד מיון פריט",
                "מכירות_בכסף",
                "מכירות_בכמות",
                "מחיר_ממוצע",
                "תוספת_יעד_כסף",
                "תוספת_יעד_כמות",
                "יעד_בכסף",
                "יעד_בכמות",
                "פער_כמות",
                "% עמידה",
            ]
        ].sort_values("מכירות_בכסף", ascending=False),
        use_container_width=True,
        hide_index=True,
        column_config={
            "מכירות_בכסף": st.column_config.NumberColumn(format="%.2f"),
            "מכירות_בכמות": st.column_config.NumberColumn(format="%.2f"),
            "מחיר_ממוצע": st.column_config.NumberColumn(format="%.2f"),
            "תוספת_יעד_כסף": st.column_config.NumberColumn(format="%.2f"),
            "תוספת_יעד_כמות": st.column_config.NumberColumn(format="%.2f"),
            "יעד_בכסף": st.column_config.NumberColumn(format="%.2f"),
            "יעד_בכמות": st.column_config.NumberColumn(format="%.2f"),
            "פער_כמות": st.column_config.NumberColumn(format="%.2f"),
            "% עמידה": st.column_config.NumberColumn(format="%.1f"),
        },
    )
    st.markdown("</div>", unsafe_allow_html=True)

    # Editable only when SINGLE customer selected
    if single:
        account = selected_customers[0]
        df_cust = df_scope.copy()

        with st.spinner("מחשב נתוני לקוח לעריכה..."):
            class_view = build_class_view(delta_qty_dict, delta_money_dict, selected_agent, account, df_cust)

        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("### 5) עריכת יעדים (לקוח יחיד)")
        st.info("✏️ ערוך רק את **תוספת יעד (כמות)**. רענון = חישוב בלבד. שמירה = כתיבה ל-SQLite.")

        form_key = f"targets_form::{selected_agent}::{account}"
        with st.form(key=form_key, clear_on_submit=False):
            edited = st.data_editor(
                class_view.sort_values("מכירות_בכסף", ascending=False).reset_index(drop=True),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "שם קוד מיון פריט": st.column_config.TextColumn("שם קוד מיון", disabled=True),
                    "מכירות_בכסף": st.column_config.NumberColumn("מכירות (₪)", disabled=True, format="%.2f"),
                    "מכירות_בכמות": st.column_config.NumberColumn("מכירות (כמות)", disabled=True, format="%.2f"),
                    "מחיר_ממוצע": st.column_config.NumberColumn("מחיר ממוצע", disabled=True, format="%.2f"),
                    "תוספת_יעד_כמות": st.column_config.NumberColumn("תוספת יעד (כמות)", step=1.0, format="%.2f"),
                    "תוספת_יעד_כסף": st.column_config.NumberColumn("תוספת יעד (₪) — מחושב", disabled=True, format="%.2f"),
                    "יעד_בכסף": st.column_config.NumberColumn("יעד 2026 (₪) — מחושב", disabled=True, format="%.2f"),
                    "יעד_בכמות": st.column_config.NumberColumn("יעד 2026 (כמות) — מחושב", disabled=True, format="%.2f"),
                    "פער_כמות": st.column_config.NumberColumn("פער כמות", disabled=True, format="%.2f"),
                    "% עמידה": st.column_config.NumberColumn("% עמידה", disabled=True, format="%.1f"),
                },
                key=f"class_editor_qty::{selected_agent}::{account}",
            )

            b1, b2 = st.columns([1, 1], gap="small")
            with b1:
                refresh_clicked = st.form_submit_button("רענן חישוב יעדים", use_container_width=True)
            with b2:
                save_clicked = st.form_submit_button("שמור למסד", use_container_width=True)

        if refresh_clicked or save_clicked:
            edited["תוספת_יעד_כמות"] = pd.to_numeric(edited["תוספת_יעד_כמות"], errors="coerce").fillna(0.0)

            for _, r in edited.iterrows():
                cls = str(r["שם קוד מיון פריט"])
                dq = float(r["תוספת_יעד_כמות"] or 0.0)
                key = (str(selected_agent), str(account), cls)
                delta_qty_dict[key] = dq
                if save_clicked:
                    db_upsert_qty(con, str(selected_agent), str(account), cls, dq)

            st.success("✅ נשמר למסד + חישובים עודכנו" if save_clicked else "✅ חישובים עודכנו (ללא שמירה למסד)")

        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("#### ⬇️ ייצוא דוח לקוח (Excel)")
        st.caption("ירוק רק בקודים שבהם הוזנה תוספת יעד (כמות).")

        filename = f"uzeb_{safe_filename(selected_agent)}__{safe_filename(account)}__classes.xlsx"
        export_classes = build_class_view(delta_qty_dict, delta_money_dict, selected_agent, account, df_cust).copy()
        xls = make_styled_export_excel(agent_label(selected_agent), str(account), export_classes)

        st.download_button(
            "הורד דוח לקוח (Excel)",
            data=xls,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)
