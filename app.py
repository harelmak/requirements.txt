# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v5.1 (FULL FILE, RTL, Improved UX/UI)
Deploy-ready for Streamlit Community Cloud.

Features:
- Upload Excel -> Choose Agent -> Choose Customers (multi + Select All)
- Single customer:
    - Edit ONLY תוספת_יעד_כמות inside a FORM
    - Buttons next to table:
        1) רענן חישוב יעדים (updates calculations + KPI, NO DB write)
        2) שמור למסד (updates calculations + writes to SQLite)
- Multi customers:
    - Combined view (read-only)
    - Export report Excel (Summary + Combined + per-customer sheets)
- Single customer:
    - Styled Excel export

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
h2 { margin-top: 0.1rem; }
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


def kpi_block(display_sales: float, base_sales: float, added_money: float, pct_growth: float):
    st.markdown(
        f"""
        <div class="kpi-grid">
            <div class="kpi">
                <div class="label">סה״כ יעד/מכירות מתוקן (₪)</div>
                <div class="value">{fmt_money(display_sales)}</div>
                <div class="sub">מכירות + תוספות יעד כסף</div>
            </div>
            <div class="kpi">
                <div class="label">מכירות מקוריות (₪)</div>
                <div class="value">{fmt_money(base_sales)}</div>
                <div class="sub">סכום נטו מהקובץ</div>
            </div>
            <div class="kpi">
                <div class="label">סה״כ תוספות יעד (₪)</div>
                <div class="value">{fmt_money(added_money)}</div>
                <div class="sub">Σ(תוספת כמות × מחיר ממוצע)</div>
            </div>
            <div class="kpi">
                <div class="label">גידול יעד (%)</div>
                <div class="value">{fmt_pct(pct_growth)}</div>
                <div class="sub">תוספות כסף ÷ מכירות מקוריות</div>
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
# Export: Single customer (styled)
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

        dm_col_idx = cols.index("תוספת_יעד_כסף") + start_col
        dm_cell = ws.cell(row=i, column=dm_col_idx)
        try:
            dm_val = float(dm_cell.value) if dm_cell.value is not None else 0.0
            if dm_val > 0:
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
# Export: Multi customers report
# =========================
def _write_df_to_sheet(ws, df: pd.DataFrame, rtl=True, freeze="A2"):
    ws.sheet_view.rightToLeft = rtl
    thin = Side(style="thin", color="D0D0D0")
    border_all = Border(left=thin, right=thin, top=thin, bottom=thin)

    for j, col in enumerate(df.columns, start=1):
        c = ws.cell(row=1, column=j, value=col)
        c.font = Font(bold=True)
        c.fill = PatternFill("solid", fgColor="F3F4F6")
        c.alignment = Alignment(horizontal="center", vertical="center")
        c.border = border_all

    for i, row in enumerate(df.itertuples(index=False), start=2):
        for j, v in enumerate(row, start=1):
            c = ws.cell(row=i, column=j, value=v)
            c.alignment = Alignment(horizontal="right" if j == 1 else "center", vertical="center")
            c.border = border_all

    for j, col in enumerate(df.columns, start=1):
        letter = get_column_letter(j)
        max_len = max([len(str(col))] + [len(str(x)) for x in df[col].head(200).tolist()])
        ws.column_dimensions[letter].width = min(max(10, max_len + 2), 45)

    if freeze:
        ws.freeze_panes = ws[freeze]


def make_targets_report_excel_for_selection(
    agent_raw: str,
    agent_display: str,
    customers: list[str],
    agent_df: pd.DataFrame,
    delta_qty_dict: dict,
    delta_money_dict: dict,
    per_customer_cap: int = 30,
) -> bytes:
    wb = Workbook()
    wb.remove(wb.active)

    # Summary per customer
    summary_rows = []
    for acc in customers:
        df_c = agent_df[agent_df[COL_ACCOUNT].astype(str) == str(acc)].copy()
        if df_c.empty:
            continue
        class_view = build_class_view(delta_qty_dict, delta_money_dict, agent_raw, str(acc), df_c)

        base_sales = float(class_view["מכירות_בכסף"].sum())
        added_money = float(pd.to_numeric(class_view["תוספת_יעד_כסף"], errors="coerce").fillna(0.0).sum())
        target_sales = base_sales + added_money
        pct_growth = safe_div(added_money, base_sales) * 100 if base_sales > 0 else math.nan

        summary_rows.append(
            {
                "סוכן": agent_display,
                "לקוח": str(acc),
                "מכירות מקוריות (₪)": base_sales,
                "תוספת יעד (₪)": added_money,
                "יעד/מתוקן (₪)": target_sales,
                "גידול יעד (%)": pct_growth,
            }
        )

    df_summary = pd.DataFrame(summary_rows)
    ws = wb.create_sheet("סיכום לקוחות")
    _write_df_to_sheet(ws, df_summary if not df_summary.empty else pd.DataFrame([{"אין נתונים": ""}]))

    # Combined classes across selection
    df_sel = agent_df[agent_df[COL_ACCOUNT].astype(str).isin([str(x) for x in customers])].copy()
    if not df_sel.empty:
        combined = compute_classes(df_sel).rename(columns={COL_CLASS: "שם קוד מיון פריט"})

        def agg_qty_delta(cls: str) -> float:
            total = 0.0
            for (ag, acc, c), dq in delta_qty_dict.items():
                if str(ag) != str(agent_raw):
                    continue
                if str(acc) not in [str(x) for x in customers]:
                    continue
                if str(c) == str(cls):
                    total += float(dq or 0.0)
            return total

        combined["תוספת_יעד_כמות"] = combined["שם קוד מיון פריט"].astype(str).apply(agg_qty_delta)

        def qty_to_money_row(r):
            p = r["מחיר_ממוצע"]
            dq = float(r["תוספת_יעד_כמות"] or 0.0)
            if pd.isna(p) or float(p) == 0:
                return math.nan
            return dq * float(p)

        combined["תוספת_יעד_כסף"] = combined.apply(qty_to_money_row, axis=1)
        combined["יעד_בכמות"] = combined["מכירות_בכמות"] + combined["תוספת_יעד_כמות"]
        combined["יעד_בכסף"] = combined.apply(
            lambda r: float(r["מכירות_בכסף"] or 0.0) + (0.0 if pd.isna(r["תוספת_יעד_כסף"]) else float(r["תוספת_יעד_כסף"])),
            axis=1,
        )
        combined["פער_כמות"] = combined["יעד_בכמות"] - combined["מכירות_בכמות"]
        combined["% עמידה"] = combined.apply(
            lambda r: (r["מכירות_בכסף"] / r["יעד_בכסף"] * 100) if float(r["יעד_בכסף"] or 0) > 0 else math.nan,
            axis=1,
        )

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
        ws = wb.create_sheet("קודי מיון - משולב")
        _write_df_to_sheet(ws, combined[cols].sort_values("מכירות_בכסף", ascending=False).reset_index(drop=True))

    for acc in customers[:per_customer_cap]:
        df_c = agent_df[agent_df[COL_ACCOUNT].astype(str) == str(acc)].copy()
        if df_c.empty:
            continue
        class_view = build_class_view(delta_qty_dict, delta_money_dict, agent_raw, str(acc), df_c)
        ws = wb.create_sheet(safe_filename(str(acc))[:31])
        _write_df_to_sheet(ws, class_view.sort_values("מכירות_בכסף", ascending=False).reset_index(drop=True))

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
  <p>העלה קובץ Excel, בחר סוכן ולקוחות, עדכן תוספת יעד בכמות, והורד דוחות.</p>
</div>
""",
    unsafe_allow_html=True,
)

# =========================
# Sidebar
# =========================
with st.sidebar:
    st.markdown("### שלבים")
    st.caption("1) העלה קובץ  →  2) בחר סוכן  →  3) בחר לקוחות  →  4) עריכה/ייצוא")

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
agent_total_money = float(agent_df[COL_NET].sum())

# Customers summary
cust_table = (
    agent_df.groupby(COL_ACCOUNT)
    .agg(סהכ_כסף=(COL_NET, "sum"), סהכ_כמות=(COL_QTY, "sum"))
    .reset_index()
    .sort_values("סהכ_כסף", ascending=False)
    .reset_index(drop=True)
)
cust_table["נתח_ממכירות_הסוכן"] = cust_table["סהכ_כסף"].apply(
    lambda x: safe_div(float(x), agent_total_money) * 100 if agent_total_money > 0 else math.nan
)

sel_key = f"cust_selection::{selected_agent}"
all_key = f"cust_select_all::{selected_agent}"
if sel_key not in st.session_state:
    st.session_state[sel_key] = set([cust_table[COL_ACCOUNT].iloc[0]]) if len(cust_table) else set()
if all_key not in st.session_state:
    st.session_state[all_key] = False

left, right = st.columns([1, 2], gap="large")

with left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### 2) בחירת לקוחות")
    st.caption("סמן ✅ לקוח/ים. אם תבחר יותר מלקוח אחד — תוצג תצוגה משולבת + דו״ח להורדה.")

    select_all = st.checkbox("בחר הכל", value=st.session_state[all_key])
    st.session_state[all_key] = select_all

    cust_view = cust_table[[COL_ACCOUNT, "סהכ_כסף", "סהכ_כמות", "נתח_ממכירות_הסוכן"]].copy()
    cust_view.insert(0, "בחר", False)

    if select_all:
        cust_view["בחר"] = True
        st.session_state[sel_key] = set(cust_table[COL_ACCOUNT].tolist())
    else:
        cust_view["בחר"] = cust_view[COL_ACCOUNT].apply(lambda a: a in st.session_state[sel_key])

    edited_customers = st.data_editor(
        cust_view,
        hide_index=True,
        use_container_width=True,
        column_config={
            "בחר": st.column_config.CheckboxColumn("בחר"),
            COL_ACCOUNT: st.column_config.TextColumn("שם לקוח", disabled=True),
            "סהכ_כסף": st.column_config.NumberColumn("סה״כ כסף", disabled=True, format="%.2f"),
            "סהכ_כמות": st.column_config.NumberColumn("סה״כ כמות", disabled=True, format="%.2f"),
            "נתח_ממכירות_הסוכן": st.column_config.NumberColumn("נתח (%)", disabled=True, format="%.1f"),
        },
        key=f"customers_editor::{selected_agent}",
    )

    new_selected = set(edited_customers.loc[edited_customers["בחר"] == True, COL_ACCOUNT].astype(str).tolist())
    if not new_selected and len(cust_table):
        new_selected = {cust_table[COL_ACCOUNT].iloc[0]}
    st.session_state[sel_key] = new_selected

    st.markdown("</div>", unsafe_allow_html=True)

with right:
    selected_customers = sorted(list(st.session_state[sel_key]))
    multi = len(selected_customers) != 1

    df_sel = agent_df[agent_df[COL_ACCOUNT].isin(selected_customers)].copy()
    sel_base_sales = float(df_sel[COL_NET].sum())
    sel_share_pct = safe_div(sel_base_sales, agent_total_money) * 100 if agent_total_money > 0 else math.nan
    title_customers = selected_customers[0] if len(selected_customers) == 1 else f"{len(selected_customers)} לקוחות"

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### 3) תצוגה ופעולות")
    st.caption(f"סוכן: {agent_label(selected_agent)} | לקוחות: {title_customers} | נתח מכירות: {fmt_pct(sel_share_pct)}")
    st.markdown("</div>", unsafe_allow_html=True)

    # MULTI customers
    if multi:
        st.info("תצוגה משולבת: לא ניתן לערוך כאן. לעריכה בחר לקוח יחיד.")

        with st.spinner("מחשב תצוגה משולבת..."):
            class_sales = compute_classes(df_sel).rename(columns={COL_CLASS: "שם קוד מיון פריט"})

            def agg_qty_delta(cls: str) -> float:
                total = 0.0
                for (ag, acc, c), dq in delta_qty_dict.items():
                    if str(ag) != str(selected_agent):
                        continue
                    if str(acc) not in [str(x) for x in selected_customers]:
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

        base_sales = float(class_sales["מכירות_בכסף"].sum())
        added_money = float(pd.to_numeric(class_sales["תוספת_יעד_כסף"], errors="coerce").fillna(0.0).sum())
        display_sales = base_sales + added_money
        pct_growth = safe_div(added_money, base_sales) * 100 if base_sales > 0 else math.nan
        kpi_block(display_sales, base_sales, added_money, pct_growth)

        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("#### טבלת קודי מיון (משולב)")
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
        )
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("#### ⬇️ ייצוא דו״ח יעדים")
        st.caption("כולל: סיכום לקוחות + קודי מיון משולב + דף לכל לקוח (עד 30).")

        report_name = f"uzeb_{safe_filename(selected_agent)}__{len(selected_customers)}_customers__targets.xlsx"
        report_bytes = make_targets_report_excel_for_selection(
            agent_raw=str(selected_agent),
            agent_display=agent_label(selected_agent),
            customers=[str(x) for x in selected_customers],
            agent_df=agent_df,
            delta_qty_dict=delta_qty_dict,
            delta_money_dict=delta_money_dict,
            per_customer_cap=30,
        )
        st.download_button(
            "הורד דו״ח יעדים (Excel)",
            data=report_bytes,
            file_name=report_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)

        st.stop()

    # SINGLE customer
    account = selected_customers[0]
    df_cust = agent_df[agent_df[COL_ACCOUNT].astype(str) == str(account)].copy()

    with st.spinner("מחשב נתוני לקוח..."):
        class_view = build_class_view(delta_qty_dict, delta_money_dict, selected_agent, account, df_cust)

    base_sales = float(class_view["מכירות_בכסף"].sum())
    added_money = float(pd.to_numeric(class_view["תוספת_יעד_כסף"], errors="coerce").fillna(0.0).sum())
    display_sales = base_sales + added_money
    pct_growth = safe_div(added_money, base_sales) * 100 if base_sales > 0 else math.nan
    kpi_block(display_sales, base_sales, added_money, pct_growth)

    # ======= EDIT AREA (FORM) =======
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("#### 4) עריכת יעדים (לקוח יחיד)")
    st.info("✏️ ערוך רק את **תוספת יעד (כמות)**. לחץ **רענן חישוב יעדים** לעדכון חישובים/KPI (ללא שמירה). לחץ **שמור למסד** כדי לשמור SQLite.")

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
                "יעד_בכסף": st.column_config.NumberColumn("יעד סופי (₪) — מחושב", disabled=True, format="%.2f"),
                "יעד_בכמות": st.column_config.NumberColumn("יעד סופי (כמות) — מחושב", disabled=True, format="%.2f"),
                "פער_כמות": st.column_config.NumberColumn("פער כמות", disabled=True, format="%.2f"),
                "% עמידה": st.column_config.NumberColumn("% עמידה", disabled=True, format="%.1f"),
            },
            key=f"class_editor_qty::{selected_agent}::{account}",
        )

        b1, b2, _ = st.columns([2, 2, 6], gap="small")
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

        class_view = build_class_view(delta_qty_dict, delta_money_dict, selected_agent, account, df_cust)

        base_sales = float(class_view["מכירות_בכסף"].sum())
        added_money = float(pd.to_numeric(class_view["תוספת_יעד_כסף"], errors="coerce").fillna(0.0).sum())
        display_sales = base_sales + added_money
        pct_growth = safe_div(added_money, base_sales) * 100 if base_sales > 0 else math.nan
        kpi_block(display_sales, base_sales, added_money, pct_growth)

        st.success("✅ נשמר למסד + חישובים עודכנו" if save_clicked else "✅ חישובים עודכנו (ללא שמירה למסד)")

    st.markdown("</div>", unsafe_allow_html=True)

    # Items detail (optional)
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("#### פירוט פריטים (אופציונלי)")
    if COL_ITEM not in df_cust.columns:
        st.caption('לא נמצאה עמודה "שם פריט" בקובץ — לא ניתן להציג פירוט פריטים.')
    else:
        c1, c2 = st.columns([2, 1], gap="small")
        with c1:
            q = st.text_input("חיפוש בשם פריט", value="")
        with c2:
            class_filter = st.multiselect(
                "סינון לפי קוד מיון",
                options=sorted(df_cust[COL_CLASS].dropna().astype(str).unique().tolist()),
                default=[],
            )

        items_df = df_cust.copy()
        if class_filter:
            items_df = items_df[items_df[COL_CLASS].astype(str).isin([str(x) for x in class_filter])]
        if q.strip():
            items_df = items_df[items_df[COL_ITEM].astype(str).str.contains(q.strip(), case=False, na=False)]

        items_sum = (
            items_df.groupby([COL_CLASS, COL_ITEM], dropna=False)
            .agg(מכירות_בכסף=(COL_NET, "sum"), מכירות_בכמות=(COL_QTY, "sum"))
            .reset_index()
            .sort_values("מכירות_בכסף", ascending=False)
            .reset_index(drop=True)
        )
        st.dataframe(items_sum, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # Export single customer
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("#### ⬇️ ייצוא דוח לקוח (Excel)")
    st.caption("דוח מעוצב הכולל קודי מיון, מכירות, תוספות ויעד סופי.")

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
