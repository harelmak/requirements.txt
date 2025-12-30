# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v8.4 (FULL FILE, RTL, Mobile/Tablet friendly)

Performance update (Section 1):
- Company Excel is parsed/normalized ONCE on upload by ADMIN and stored as compressed DF bytes in SQLite.
- App loads the processed DF (fast) on reruns; if missing/outdated -> self-heal (process once and store).

Auth + data isolation:
- ADMIN login: username ADMIN, password 1511!!
- ADMIN can create users, disable/hard-delete users, upload company file.
- Agents see only their agent_id rows.
- Targets (delta qty) are stored per-username.

UI:
- RTL + mobile responsive.
- Customer multiselect shows share (%) next to customer name.
- If no customer selected -> KPI for whole agent.
- If single selected -> show target editing table (class-level) + items detail by chosen class (checkbox list).
- Excel report download: per customer 2025/2026/diff/% + total row.

Run:
  streamlit run app.py
"""

import base64
import gzip
import hashlib
import hmac
import math
import os
import re
import sqlite3
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

# =========================
# ADMIN credentials
# =========================
ADMIN_USERNAME = "ADMIN"
ADMIN_PASSWORD = "1511!!"

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
COL_AGENT = "???? ??????"
COL_ACCOUNT = "?? ?????"
COL_CLASS = "?? ??? ???? ????"
COL_ITEM = "?? ????"  # optional
COL_QTY = "??? ????"
COL_NET = "??????/????? ???"

# =========================
# Agent mapping
# =========================
AGENT_NAME_MAP = {"2": "?????", "15": "????", "4": "????", "7": "????", "1": "????"}


def agent_label(agent_raw) -> str:
    a = str(agent_raw).strip()
    name = AGENT_NAME_MAP.get(a)
    return f"{a} — {name}" if name else a


# =========================
# DB (deploy-safe)
# =========================
DB_FILENAME = "uzeb_app.sqlite"
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
    con_ = sqlite3.connect(db_path.as_posix(), check_same_thread=False)

    con_.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            agent_name TEXT,
            salt_b64 TEXT NOT NULL,
            pwd_hash_b64 TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """
    )

    con_.execute(
        """
        CREATE TABLE IF NOT EXISTS company_sales_file (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            filename TEXT,
            file_bytes BLOB NOT NULL,
            uploaded_at TEXT NOT NULL
        )
        """
    )

    con_.execute(
        """
        CREATE TABLE IF NOT EXISTS company_sales_processed (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            source_uploaded_at TEXT NOT NULL,
            df_gz_bytes BLOB NOT NULL,
            created_at TEXT NOT NULL,
            nrows INTEGER NOT NULL
        )
        """
    )

    con_.execute(
        """
        CREATE TABLE IF NOT EXISTS user_sales_file (
            username TEXT PRIMARY KEY,
            filename TEXT,
            file_bytes BLOB NOT NULL,
            uploaded_at TEXT NOT NULL,
            FOREIGN KEY(username) REFERENCES users(username)
        )
        """
    )

    con_.execute(
        """
        CREATE TABLE IF NOT EXISTS user_sales_processed (
            username TEXT PRIMARY KEY,
            source_uploaded_at TEXT NOT NULL,
            df_gz_bytes BLOB NOT NULL,
            created_at TEXT NOT NULL,
            nrows INTEGER NOT NULL,
            FOREIGN KEY(username) REFERENCES users(username)
        )
        """
    )

    con_.execute(
        """
        CREATE TABLE IF NOT EXISTS user_class_delta_qty (
            username TEXT NOT NULL,
            account TEXT NOT NULL,
            cls TEXT NOT NULL,
            delta_qty REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (username, account, cls),
            FOREIGN KEY(username) REFERENCES users(username)
        )
        """
    )

    con_.commit()
    return con_


@st.cache_resource
def get_db(db_path_str: str):
    return db_connect(Path(db_path_str))


db_path = get_db_path()
con = get_db(str(db_path))

# =========================
# Serialization helpers (fast load)
# =========================
def df_to_gz_bytes(df: pd.DataFrame) -> bytes:
    bio = BytesIO()
    with gzip.GzipFile(fileobj=bio, mode="wb") as gz:
        pd.to_pickle(df, gz)
    return bio.getvalue()


def df_from_gz_bytes(b: bytes) -> pd.DataFrame:
    bio = BytesIO(b)
    with gzip.GzipFile(fileobj=bio, mode="rb") as gz:
        return pd.read_pickle(gz)


# =========================
# Auth (PBKDF2-HMAC-SHA256)
# =========================
def _pbkdf2_hash(password: str, salt: bytes, iterations: int = 200_000) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, dklen=32)


def _b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def _b64d(s: str) -> bytes:
    return base64.b64decode(s.encode("ascii"))


def create_user(con_, username: str, password: str, agent_id: str, agent_name: str = ""):
    username = str(username).strip()
    agent_id = str(agent_id).strip()
    if not username or not password or not agent_id:
        raise ValueError("username/password/agent_id required")
    if username.upper() == ADMIN_USERNAME:
        raise ValueError("ADMIN ??? ?? ????.")

    now = datetime.now(timezone.utc).isoformat()
    salt = os.urandom(16)
    pwd_hash = _pbkdf2_hash(password, salt)

    con_.execute(
        """
        INSERT INTO users(username, agent_id, agent_name, salt_b64, pwd_hash_b64, is_active, created_at)
        VALUES(?,?,?,?,?,?,?)
        """,
        (username, agent_id, agent_name, _b64e(salt), _b64e(pwd_hash), 1, now),
    )
    con_.commit()


def load_user(con_, username: str) -> Optional[dict]:
    row = con_.execute(
        "SELECT username, agent_id, agent_name, salt_b64, pwd_hash_b64, is_active FROM users WHERE username=?",
        (str(username).strip(),),
    ).fetchone()
    if not row:
        return None
    return {
        "username": row[0],
        "agent_id": row[1],
        "agent_name": row[2] or "",
        "salt_b64": row[3],
        "pwd_hash_b64": row[4],
        "is_active": int(row[5] or 0),
    }


def verify_login(con_, username: str, password: str) -> Tuple[bool, Optional[dict]]:
    uname = str(username).strip()
    pwd = str(password)

    if uname.upper() == ADMIN_USERNAME and pwd == ADMIN_PASSWORD:
        return True, {"username": ADMIN_USERNAME, "agent_id": "", "agent_name": "ADMIN", "is_admin": True}

    u = load_user(con_, uname)
    if not u or u["is_active"] != 1:
        return False, None

    salt = _b64d(u["salt_b64"])
    expected = _b64d(u["pwd_hash_b64"])
    got = _pbkdf2_hash(pwd, salt)
    if hmac.compare_digest(expected, got):
        u["is_admin"] = False
        return True, u
    return False, None


# =========================
# Sales parsing + normalize
# =========================
def detect_header_row(file_like, needle=COL_AGENT, max_rows=25) -> int:
    preview = pd.read_excel(file_like, header=None, nrows=max_rows)
    for r in range(preview.shape[0]):
        vals = [str(x).strip() for x in preview.iloc[r].tolist()]
        if needle in vals:
            return r
    return 0


def read_sales_excel_bytes(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    header_row = detect_header_row(bio)
    bio.seek(0)
    return pd.read_excel(bio, header=header_row)


def normalize_sales_strict(df: pd.DataFrame) -> pd.DataFrame:
    required = {COL_AGENT, COL_ACCOUNT, COL_CLASS, COL_QTY, COL_NET}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"????? ?????? ?????: {missing}")

    out = df.copy()
    out = out[out[COL_ACCOUNT].notna()]

    out[COL_AGENT] = out[COL_AGENT].astype(str).str.strip()
    out[COL_ACCOUNT] = out[COL_ACCOUNT].astype(str).str.strip()
    out[COL_CLASS] = out[COL_CLASS].astype(str).str.strip()

    if COL_ITEM in out.columns:
        out[COL_ITEM] = out[COL_ITEM].astype(str).str.strip()

    out[COL_QTY] = pd.to_numeric(out[COL_QTY], errors="coerce").fillna(0.0)
    out[COL_NET] = pd.to_numeric(out[COL_NET], errors="coerce").fillna(0.0)

    # perf: categories accelerate groupby
    out[COL_AGENT] = out[COL_AGENT].astype("category")
    out[COL_ACCOUNT] = out[COL_ACCOUNT].astype("category")
    out[COL_CLASS] = out[COL_CLASS].astype("category")
    if COL_ITEM in out.columns:
        out[COL_ITEM] = out[COL_ITEM].astype("category")

    return out


# =========================
# DB: Company file + processed DF
# =========================
def db_upsert_company_file(con_, filename: str, file_bytes: bytes) -> str:
    now = datetime.now(timezone.utc).isoformat()
    con_.execute(
        """
        INSERT INTO company_sales_file(id, filename, file_bytes, uploaded_at)
        VALUES(1,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            filename=excluded.filename,
            file_bytes=excluded.file_bytes,
            uploaded_at=excluded.uploaded_at
        """,
        (str(filename or ""), sqlite3.Binary(file_bytes), now),
    )
    con_.commit()
    return now


def db_load_company_file(con_) -> Optional[dict]:
    row = con_.execute("SELECT filename, file_bytes, uploaded_at FROM company_sales_file WHERE id=1").fetchone()
    if not row:
        return None
    return {"filename": row[0] or "", "file_bytes": bytes(row[1]), "uploaded_at": row[2]}


def db_upsert_company_processed(con_, source_uploaded_at: str, df_norm: pd.DataFrame):
    # FIXED: 5 columns -> VALUES(1,?,?,?,?) (not 6 placeholders)
    now = datetime.now(timezone.utc).isoformat()
    gz_bytes = df_to_gz_bytes(df_norm)
    con_.execute(
        """
        INSERT INTO company_sales_processed(id, source_uploaded_at, df_gz_bytes, created_at, nrows)
        VALUES(1,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
            source_uploaded_at=excluded.source_uploaded_at,
            df_gz_bytes=excluded.df_gz_bytes,
            created_at=excluded.created_at,
            nrows=excluded.nrows
        """,
        (str(source_uploaded_at), sqlite3.Binary(gz_bytes), now, int(len(df_norm))),
    )
    con_.commit()


def db_load_company_processed(con_) -> Optional[dict]:
    row = con_.execute(
        "SELECT source_uploaded_at, df_gz_bytes, created_at, nrows FROM company_sales_processed WHERE id=1"
    ).fetchone()
    if not row:
        return None
    return {"source_uploaded_at": row[0], "df_gz_bytes": bytes(row[1]), "created_at": row[2], "nrows": int(row[3])}


# =========================
# DB: Users list + delete
# =========================
def db_list_non_admin_users(con_) -> pd.DataFrame:
    rows = con_.execute(
        """
        SELECT username, agent_id, COALESCE(agent_name,''), is_active
        FROM users
        WHERE UPPER(username) <> ?
        ORDER BY agent_id, username
        """,
        (ADMIN_USERNAME,),
    ).fetchall()
    df = pd.DataFrame(rows, columns=["username", "agent_id", "agent_name", "is_active"])
    if df.empty:
        return df
    df["is_active"] = df["is_active"].astype(int)
    return df


def db_disable_user(con_, username: str):
    con_.execute("UPDATE users SET is_active=0 WHERE username=?", (str(username),))
    con_.commit()


def db_delete_user_targets(con_, username: str):
    con_.execute("DELETE FROM user_class_delta_qty WHERE username=?", (str(username),))
    con_.commit()


def db_hard_delete_user(con_, username: str):
    con_.execute("DELETE FROM users WHERE username=?", (str(username),))
    con_.commit()


# =========================
# DB: Per-user targets
# =========================
def db_load_user_qty(con_, username: str) -> dict:
    rows = con_.execute(
        "SELECT account, cls, delta_qty FROM user_class_delta_qty WHERE username=?",
        (str(username),),
    ).fetchall()
    return {(str(username), str(acc), str(cls)): float(dq or 0.0) for acc, cls, dq in rows}


def db_upsert_user_qty(con_, username: str, account: str, cls: str, delta_qty: float):
    now = datetime.now(timezone.utc).isoformat()
    con_.execute(
        """
        INSERT INTO user_class_delta_qty(username, account, cls, delta_qty, updated_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(username, account, cls) DO UPDATE SET
            delta_qty=excluded.delta_qty,
            updated_at=excluded.updated_at
        """,
        (str(username), str(account), str(cls), float(delta_qty or 0.0), now),
    )
    con_.commit()


# =========================
# Helpers
# =========================
def safe_div(a, b):
    if b in (0, 0.0) or pd.isna(b):
        return math.nan
    return a / b


def safe_filename(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r'[\\/:*?"<>|]+', "_", s)
    s = re.sub(r"\s+", " ", s)
    return s[:60] if len(s) > 60 else s


def fmt_money(x) -> str:
    try:
        return f"? {float(x):,.2f}"
    except Exception:
        return "? 0.00"


def fmt_pct(x) -> str:
    if x is None or pd.isna(x):
        return "—"
    return f"{float(x):,.1f}%"


def compute_classes(df: pd.DataFrame) -> pd.DataFrame:
    g = (
        df.groupby(COL_CLASS, dropna=False)
        .agg(??????_????=(COL_NET, "sum"), ??????_?????=(COL_QTY, "sum"))
        .reset_index()
        .sort_values("??????_????", ascending=False)
        .reset_index(drop=True)
    )
    g["????_?????"] = g.apply(lambda r: safe_div(r["??????_????"], r["??????_?????"]), axis=1)
    return g


def kpi_block(s2026: float, s2025: float, diff_money: float, pct: float, share_pct: Optional[float], title_2026: str):
    share_line = ""
    if share_pct is not None and not pd.isna(share_pct):
        share_line = f"<div class='sub'>??? ???? ???????? ?? ?????: {fmt_pct(share_pct)}</div>"

    st.markdown(
        f"""
        <div class="kpi-grid">
            <div class="kpi">
                <div class="label">{title_2026}</div>
                <div class="value">{fmt_money(s2026)}</div>
                {share_line}
            </div>
            <div class="kpi">
                <div class="label">?????? 2025 (?)</div>
                <div class="value">{fmt_money(s2025)}</div>
                <div class="sub">???? ??? ??????</div>
            </div>
            <div class="kpi">
                <div class="label">???? (?)</div>
                <div class="value">{fmt_money(diff_money)}</div>
                <div class="sub">2026 - 2025</div>
            </div>
            <div class="kpi">
                <div class="label">????? (%)</div>
                <div class="value">{fmt_pct(pct)}</div>
                <div class="sub">(2026/2025)*100 - 100</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =========================
# Targets logic (per-user)
# =========================
def get_delta_qty(user_qty: dict, username: str, account: str, cls: str) -> float:
    return float(user_qty.get((str(username), str(account), str(cls)), 0.0) or 0.0)


def build_class_view(user_qty: dict, username: str, account: str, df_customer: pd.DataFrame) -> pd.DataFrame:
    class_df = compute_classes(df_customer)

    class_df["?????_???_????"] = class_df.apply(
        lambda r: get_delta_qty(user_qty, username, account, str(r[COL_CLASS])),
        axis=1,
    )

    def qty_to_money(r):
        p = r["????_?????"]
        dq = float(r["?????_???_????"] or 0.0)
        if pd.isna(p) or float(p) == 0:
            return 0.0
        return dq * float(p)

    class_df["?????_???_???"] = class_df.apply(qty_to_money, axis=1)
    class_df["???_????"] = class_df["??????_????"] + class_df["?????_???_???"]
    class_df["???_?????"] = class_df["??????_?????"] + class_df["?????_???_????"]

    out = class_df[
        [
            COL_CLASS,
            "??????_????",
            "??????_?????",
            "????_?????",
            "?????_???_???",
            "?????_???_????",
            "???_????",
            "???_?????",
        ]
    ].copy()
    return out.rename(columns={COL_CLASS: "?? ??? ???? ????"})


def compute_scope_kpi(username: str, df_scope: pd.DataFrame, user_qty: dict, selected_accounts: Optional[list[str]]):
    class_sales = compute_classes(df_scope)

    scope_accounts = set(df_scope[COL_ACCOUNT].dropna().astype(str).tolist())
    if selected_accounts is None:
        allowed_accounts = scope_accounts
    else:
        allowed_accounts = set([str(x) for x in selected_accounts]) & scope_accounts

    def agg_qty_delta(cls: str) -> float:
        total = 0.0
        for (u, acc, c), dq in user_qty.items():
            if str(u) != str(username):
                continue
            if str(acc) not in allowed_accounts:
                continue
            if str(c) == str(cls):
                total += float(dq or 0.0)
        return total

    class_sales["?????_???_????"] = class_sales[COL_CLASS].astype(str).apply(agg_qty_delta)

    def qty_to_money_row(r):
        p = r["????_?????"]
        dq = float(r["?????_???_????"] or 0.0)
        if pd.isna(p) or float(p) == 0:
            return 0.0
        return dq * float(p)

    class_sales["?????_???_???"] = class_sales.apply(qty_to_money_row, axis=1)

    s2025 = float(pd.to_numeric(class_sales["??????_????"], errors="coerce").fillna(0.0).sum())
    add_money = float(pd.to_numeric(class_sales["?????_???_???"], errors="coerce").fillna(0.0).sum())
    s2026 = s2025 + add_money
    diff = s2026 - s2025
    pct = (safe_div(s2026, s2025) * 100 - 100) if s2025 > 0 else math.nan
    return s2025, s2026, diff, pct


# =========================
# Report DF + Excel
# =========================
def build_agent_sales_report_2025_2026(username: str, agent_df: pd.DataFrame, user_qty: dict) -> pd.DataFrame:
    customers = agent_df[COL_ACCOUNT].dropna().astype(str).unique().tolist()
    rows = []
    for acc in customers:
        df_c = agent_df[agent_df[COL_ACCOUNT].astype(str) == str(acc)].copy()
        if df_c.empty:
            continue
        class_view = build_class_view(user_qty, username, str(acc), df_c)
        s2025 = float(pd.to_numeric(class_view["??????_????"], errors="coerce").fillna(0.0).sum())
        add_money = float(pd.to_numeric(class_view["?????_???_???"], errors="coerce").fillna(0.0).sum())
        s2026 = s2025 + add_money
        diff = s2026 - s2025
        pct = (safe_div(s2026, s2025) * 100 - 100) if s2025 > 0 else math.nan

        rows.append(
            {
                "?? ????": str(acc),
                "?????? 2025": s2025,
                "?????? 2026": s2026,
                "???? ??? 2025 ? 2026": diff,
                "????? ???????": pct,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values("?????? 2025", ascending=False).reset_index(drop=True)

    t2025 = float(pd.to_numeric(df["?????? 2025"], errors="coerce").fillna(0.0).sum())
    t2026 = float(pd.to_numeric(df["?????? 2026"], errors="coerce").fillna(0.0).sum())
    tdiff = t2026 - t2025
    tpct = (safe_div(t2026, t2025) * 100 - 100) if t2025 > 0 else math.nan

    df_total = pd.DataFrame(
        [
            {
                "?? ????": "????",
                "?????? 2025": t2025,
                "?????? 2026": t2026,
                "???? ??? 2025 ? 2026": tdiff,
                "????? ???????": tpct,
            }
        ]
    )
    return pd.concat([df, df_total], ignore_index=True)


def make_agent_sales_excel(title: str, df_report: pd.DataFrame) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "???"
    ws.sheet_view.rightToLeft = True

    font_title = Font(bold=True, size=13)
    font_bold = Font(bold=True)
    thin = Side(style="thin", color="D0D0D0")
    border_all = Border(left=thin, right=thin, top=thin, bottom=thin)
    fill_header = PatternFill("solid", fgColor="F3F4F6")
    fill_total = PatternFill("solid", fgColor="E5E7EB")

    ws.merge_cells("A1:E1")
    ws["A1"].value = title
    ws["A1"].font = font_title
    ws["A1"].alignment = Alignment(horizontal="right", vertical="center")
    ws.row_dimensions[1].height = 22

    start_row = 3
    cols = ["?? ????", "?????? 2025", "?????? 2026", "???? ??? 2025 ? 2026", "????? ???????"]

    df = df_report.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    for j, col_name in enumerate(cols, start=1):
        cell = ws.cell(row=start_row, column=j, value=col_name)
        cell.font = font_bold
        cell.fill = fill_header
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = border_all

    for i, row in enumerate(df.itertuples(index=False), start=start_row + 1):
        is_total = (str(row[0]).strip() == "????")
        for j, value in enumerate(row, start=1):
            c = ws.cell(row=i, column=j, value=value)
            c.border = border_all
            c.alignment = Alignment(horizontal="right" if j == 1 else "center", vertical="center")
            if j in (2, 3, 4):
                c.number_format = "#,##0.00"
            elif j == 5:
                c.number_format = "0.0"
            if is_total:
                c.font = font_bold
                c.fill = fill_total

    widths = {1: 34, 2: 18, 3: 18, 4: 22, 5: 16}
    for j, w in widths.items():
        ws.column_dimensions[get_column_letter(j)].width = w
    ws.freeze_panes = ws["A4"]

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


# =========================
# FAST LOAD: company DF from processed table (self-heal)
# =========================
@st.cache_data(show_spinner=False)
def load_company_sales_df_cached(source_uploaded_at: str, gz_bytes: bytes) -> pd.DataFrame:
    return df_from_gz_bytes(gz_bytes)


def get_company_sales_df(con_) -> pd.DataFrame:
    company = db_load_company_file(con_)
    if company is None:
        raise ValueError("??? ???? ???? ??????.")

    proc = db_load_company_processed(con_)
    if proc is not None and str(proc["source_uploaded_at"]) == str(company["uploaded_at"]):
        return load_company_sales_df_cached(proc["source_uploaded_at"], proc["df_gz_bytes"])

    # self-heal once
    df_raw = read_sales_excel_bytes(company["file_bytes"])
    df_norm = normalize_sales_strict(df_raw)
    db_upsert_company_processed(con_, company["uploaded_at"], df_norm)
    proc2 = db_load_company_processed(con_)
    return load_company_sales_df_cached(proc2["source_uploaded_at"], proc2["df_gz_bytes"])


# =========================
# Header
# =========================
st.markdown(
    """
<div class="card">
  <h2>?? Uzeb — ????? ???? ??????</h2>
  <p>???? ????? ??? ??? ?????. ???? ???? ?? ?? ???. ADMIN ???? ????? ??? ????? ??? ????/?????.</p>
</div>
""",
    unsafe_allow_html=True,
)

# =========================
# Sidebar: DB path
# =========================
with st.sidebar:
    st.markdown("### ????? (SQLite)")
    st.text_input("???? ?????? ???? ??????", key="db_dir")
    new_db_path = get_db_path()
    st.caption(f"DB: {new_db_path.as_posix()}")

    if "db_path_last" not in st.session_state:
        st.session_state["db_path_last"] = str(db_path)

    if str(new_db_path) != st.session_state["db_path_last"]:
        db_path = new_db_path
        con = get_db(str(db_path))
        st.session_state["db_path_last"] = str(db_path)
        st.rerun()

# =========================
# Sidebar: Login (username dropdown)
# =========================
with st.sidebar:
    st.markdown("---")
    st.markdown("### ?????")

    if st.session_state.get("logged_in") != True:
        users_df = db_list_non_admin_users(con)
        if not users_df.empty:
            users_df = users_df[users_df["is_active"] == 1].copy()

        usernames = []
        if users_df is not None and not users_df.empty:
            usernames = sorted(users_df["username"].astype(str).unique().tolist(), key=lambda x: x.lower())

        login_options = [ADMIN_USERNAME] + usernames

        u_in = st.selectbox("?? ?????", options=login_options, index=0, key="login_user")
        p_in = st.text_input("?????", type="password", key="login_pass")

        if st.button("?????", use_container_width=True):
            ok, u = verify_login(con, u_in, p_in)
            if not ok:
                st.error("?? ?????/????? ?? ?????? ?? ????? ?? ????.")
                st.stop()
            st.session_state["logged_in"] = True
            st.session_state["login_username"] = str(u["username"])
            st.session_state["is_admin"] = bool(u.get("is_admin", False))
            st.session_state["agent_id"] = str(u.get("agent_id", "") or "").strip()
            st.session_state["agent_name"] = str(u.get("agent_name", "") or "").strip()
            st.rerun()
    else:
        is_admin = bool(st.session_state.get("is_admin", False))
        st.success(f"?????: {st.session_state.get('login_username')}" + (" (ADMIN)" if is_admin else ""))
        if not is_admin:
            st.caption(f"????: {agent_label(st.session_state.get('agent_id'))}")
        if st.button("?????", use_container_width=True):
            st.session_state.clear()
            st.rerun()

# =========================
# Sidebar: ADMIN actions
# =========================
with st.sidebar:
    if st.session_state.get("logged_in") and st.session_state.get("is_admin"):
        st.markdown("---")
        st.markdown("### ADMIN — ???? ???? (?????)")

        company_saved = db_load_company_file(con)
        if company_saved is not None:
            st.caption(f"???? ???? ????: {company_saved['filename'] or 'company.xlsx'} | ?????: {company_saved['uploaded_at']}")
            proc = db_load_company_processed(con)
            if proc is not None and proc["source_uploaded_at"] == company_saved["uploaded_at"]:
                st.caption(f"? ?????? ??????? ?????? (n={proc['nrows']}) | ????: {proc['created_at']}")
            else:
                st.caption("?? ??? ?????? ??????? ?????? (?????? ????????).")
        else:
            st.caption("??? ???? ???? ???? ?????.")

        up_company = st.file_uploader("????/???? ???? ???? (.xlsx)", type=["xlsx"], key="company_uploader")
        if up_company is not None:
            try:
                raw_bytes = up_company.getvalue()
                uploaded_at = db_upsert_company_file(con, up_company.name, raw_bytes)

                # preprocess once now
                df_raw = read_sales_excel_bytes(raw_bytes)
                df_norm = normalize_sales_strict(df_raw)
                db_upsert_company_processed(con, uploaded_at, df_norm)

                st.success("???? ???? ???? + ????? ?????? ????.")
                st.rerun()
            except Exception as e:
                st.error(f"????? ??????/?????: {e}")
                st.stop()

        st.markdown("---")
        st.markdown("### ADMIN — ????? ???????")
        new_u = st.text_input("????? ???", key="admin_new_u")
        new_p = st.text_input("????? ????", type="password", key="admin_new_p")
        new_agent = st.text_input("???? ???? (agent_id)", key="admin_new_agent")
        new_agent_name = st.text_input("?? ???? (?????????)", key="admin_new_agent_name")
        if st.button("??? ?????", use_container_width=True):
            try:
                create_user(con, new_u, new_p, new_agent, new_agent_name)
                st.success("???? ?????.")
                st.rerun()
            except sqlite3.IntegrityError:
                st.error("?? ????? ??? ????.")
            except Exception as e:
                st.error(f"?????: {e}")

        st.markdown("---")
        st.markdown("### ADMIN — ????? ?????")

        users_df_all = db_list_non_admin_users(con)
        if users_df_all.empty:
            st.caption("??? ??????? ??????.")
        else:
            users_df_all["label"] = users_df_all.apply(
                lambda r: f"{r['username']} | {agent_label(r['agent_id'])}" + (" | ?? ????" if int(r["is_active"]) != 1 else ""),
                axis=1,
            )
            labels = users_df_all["label"].tolist()
            label_to_user = dict(zip(users_df_all["label"].tolist(), users_df_all["username"].tolist()))

            chosen_label = st.selectbox("??? ????? ??????", options=labels, key="admin_delete_pick")
            del_user = str(label_to_user[chosen_label])

            mode = st.radio(
                "??? ?????",
                options=[
                    "???? ????? (?????) — ?? ???? ??????",
                    "????? ???? (?????) — ????? ????? + ?????",
                ],
                index=0,
                key="admin_delete_mode",
            )

            wipe_targets = st.checkbox("????? ?? ????? (targets) ?? ??????", value=True, key="admin_wipe_targets")
            confirm_text = st.text_input("?????? DELETE ??? ????", key="admin_delete_confirm")

            if st.button("??? ?????", use_container_width=True, key="admin_delete_btn"):
                if confirm_text.strip().upper() != "DELETE":
                    st.error("?? ????. ?? ?????? DELETE.")
                    st.stop()

                try:
                    if mode.startswith("????"):
                        db_disable_user(con, del_user)
                        if wipe_targets:
                            db_delete_user_targets(con, del_user)
                        st.success("?????? ?????. (??? ????? ????? ??? ??????)")
                    else:
                        if wipe_targets:
                            db_delete_user_targets(con, del_user)
                        db_hard_delete_user(con, del_user)
                        st.success("?????? ???? ???????.")
                    st.rerun()
                except Exception as e:
                    st.error(f"????? ??????: {e}")

# =========================
# Require login
# =========================
if st.session_state.get("logged_in") != True:
    st.info("?? ?? ?????? ???? ??? ??????.")
    st.stop()

IS_ADMIN = bool(st.session_state.get("is_admin", False))

# =========================
# Admin view mode
# =========================
admin_company_wide = False
if IS_ADMIN:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### ADMIN — ??? ?????")
    admin_company_wide = st.checkbox("????? ???? ???? (?? ???????)", value=False)
    st.caption("?????? ???? ????: ??? ????? (targets) — ????? ?????? ????.")
    st.markdown("</div>", unsafe_allow_html=True)

# =========================
# Resolve context user/agent
# =========================
if not IS_ADMIN:
    context_username = str(st.session_state["login_username"])
    context_agent_id = str(st.session_state["agent_id"]).strip()
else:
    if admin_company_wide:
        context_username = ADMIN_USERNAME
        context_agent_id = "__ALL__"
    else:
        users_df = db_list_non_admin_users(con)
        users_df = users_df[users_df["is_active"] == 1].copy()
        if users_df.empty:
            st.error("??? ??????? ?????? ?????? (???? ADMIN).")
            st.stop()

        agent_ids = sorted(users_df["agent_id"].astype(str).unique().tolist(), key=lambda x: str(x))
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("### ADMIN — ????? ??? ????/?????")
        chosen_agent_id = st.selectbox("??? ????", options=agent_ids, format_func=agent_label, key="admin_agent_filter")

        cand = users_df[users_df["agent_id"].astype(str) == str(chosen_agent_id)].copy()
        cand["label"] = cand.apply(
            lambda r: f"{r['username']} | {agent_label(r['agent_id'])}" + (f" | {r['agent_name']}" if r["agent_name"] else ""),
            axis=1,
        )
        labels = cand["label"].tolist()
        if not labels:
            st.error("??? ??????? ?????? ????? ?????.")
            st.stop()
        chosen_label = st.selectbox("??? ?????", options=labels, key="admin_user_pick_label")
        label_to_user = {cand.iloc[i]["label"]: cand.iloc[i]["username"] for i in range(len(cand))}
        context_username = str(label_to_user[chosen_label])
        context_agent_id = str(chosen_agent_id).strip()
        st.caption(f"???? ?????? ????: {context_username} | {agent_label(context_agent_id)}")
        st.markdown("</div>", unsafe_allow_html=True)

# =========================
# Load data (FAST)
# =========================
company_saved = db_load_company_file(con)
if company_saved is None:
    st.error("??? ???? ???? ??????. ADMIN ???? ?????? ???? ???? ?????.")
    st.stop()

with st.spinner("???? ??????..."):
    sales_all = get_company_sales_df(con)

# =========================
# Scope data (agent/company)
# =========================
if IS_ADMIN and admin_company_wide:
    scope_df = sales_all.copy()
    scope_title = "???? ???? (?? ???????)"
    scope_agent_display = "?? ?????"
else:
    if not context_agent_id:
        st.error("?????? ??? agent_id. ADMIN ???? ????? ????? ?? agent_id.")
        st.stop()
    scope_df = sales_all[sales_all[COL_AGENT].astype(str) == str(context_agent_id)].copy()
    if scope_df.empty:
        st.error(f"?? ????? ?????? ????? {agent_label(context_agent_id)} ????? ???????.")
        st.stop()
    scope_title = f"????: {agent_label(context_agent_id)}"
    scope_agent_display = agent_label(context_agent_id)

# =========================
# Load targets (per user) if relevant
# =========================
user_qty = {}
qty_key = f"user_qty::{context_username}"
if not (IS_ADMIN and admin_company_wide):
    if qty_key not in st.session_state:
        st.session_state[qty_key] = db_load_user_qty(con, context_username)
    user_qty = st.session_state[qty_key]

# =========================
# Main UI
# =========================
st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown(f"### ?????? — {scope_title}")
st.markdown("</div>", unsafe_allow_html=True)

scope_total_2025 = float(pd.to_numeric(scope_df[COL_NET], errors="coerce").fillna(0.0).sum())

cust_table = (
    scope_df.groupby(COL_ACCOUNT)
    .agg(???_???=(COL_NET, "sum"), ???_????=(COL_QTY, "sum"))
    .reset_index()
    .sort_values("???_???", ascending=False)
    .reset_index(drop=True)
)
cust_table["??? (%)"] = cust_table["???_???"].apply(
    lambda x: safe_div(float(x), scope_total_2025) * 100 if scope_total_2025 > 0 else math.nan
)

share_map = dict(zip(cust_table[COL_ACCOUNT].astype(str).tolist(), cust_table["??? (%)"].tolist()))
customer_options = cust_table[COL_ACCOUNT].astype(str).tolist()


def customer_format(acc: str) -> str:
    p = share_map.get(str(acc))
    return f"{acc} — {fmt_pct(p)}"


sel_key = f"cust_selection::{context_username}::{context_agent_id}::{admin_company_wide}"
if sel_key not in st.session_state:
    st.session_state[sel_key] = []

left, right = st.columns([1, 2], gap="large")

with left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### ????? ?????? (?????????)")
    st.caption("????? ????: ?? ???? ???? ? KPI ???? ???? ?? ???????. ?????? ????? ??? ???? ????.")
    st.session_state[sel_key] = st.multiselect(
        "?????? (?? ??? ??? ???)",
        options=customer_options,
        default=st.session_state.get(sel_key, []),
        format_func=customer_format,
        key=f"ms_customers::{context_username}::{context_agent_id}::{admin_company_wide}",
    )

    st.markdown("#### ???? ?????? — 2025")
    st.dataframe(
        cust_table[[COL_ACCOUNT, "???_???", "???_????", "??? (%)"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            COL_ACCOUNT: st.column_config.TextColumn("?? ????"),
            "???_???": st.column_config.NumberColumn("?????? 2025 (?)", format="%.2f"),
            "???_????": st.column_config.NumberColumn("????", format="%.2f"),
            "??? (%)": st.column_config.NumberColumn("??? (%)", format="%.1f"),
        },
    )
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    selected_customers = [str(x) for x in st.session_state.get(sel_key, [])]
    none_selected = len(selected_customers) == 0
    single = len(selected_customers) == 1
    multi = len(selected_customers) > 1

    if none_selected:
        df_scope = scope_df.copy()
        scope_subtitle = "?? ???????"
        share_pct = None
        selected_accounts_for_scope = None
    elif single:
        df_scope = scope_df[scope_df[COL_ACCOUNT].astype(str) == str(selected_customers[0])].copy()
        scope_subtitle = f"????: {selected_customers[0]}"
        cust_sales_2025 = float(pd.to_numeric(df_scope[COL_NET], errors="coerce").fillna(0.0).sum())
        share_pct = safe_div(cust_sales_2025, scope_total_2025) * 100 if scope_total_2025 > 0 else math.nan
        selected_accounts_for_scope = [str(selected_customers[0])]
    else:
        df_scope = scope_df[scope_df[COL_ACCOUNT].astype(str).isin(selected_customers)].copy()
        scope_subtitle = f"{len(selected_customers)} ?????? (?????)"
        share_pct = None
        selected_accounts_for_scope = [str(x) for x in selected_customers]

    if df_scope.empty:
        st.error("?? ????? ?????? ?????? ??????.")
        st.stop()

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### KPI")
    st.caption(scope_subtitle)
    st.markdown("</div>", unsafe_allow_html=True)

    if IS_ADMIN and admin_company_wide:
        s2025 = float(pd.to_numeric(df_scope[COL_NET], errors="coerce").fillna(0.0).sum())
        s2026 = s2025
        diff = 0.0
        pct = 0.0 if s2025 > 0 else math.nan
        kpi_block(s2026, s2025, diff, pct, share_pct if single else None, title_2026="?????? 2026 (?) — ??? ?????")
    else:
        s2025, s2026, diff, pct = compute_scope_kpi(context_username, df_scope, user_qty, selected_accounts_for_scope)
        kpi_block(s2026, s2025, diff, pct, share_pct if single else None, title_2026="??????/??? 2026 (?)")

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown(f"### ????? ??? ???? — {scope_agent_display}")
    if IS_ADMIN and admin_company_wide:
        rep = (
            df_scope.groupby(COL_ACCOUNT)
            .agg(**{"?????? 2025": (COL_NET, "sum")})
            .reset_index()
            .rename(columns={COL_ACCOUNT: "?? ????"})
            .sort_values("?????? 2025", ascending=False)
            .reset_index(drop=True)
        )
        rep["?????? 2026"] = rep["?????? 2025"]
        rep["???? ??? 2025 ? 2026"] = 0.0
        rep["????? ???????"] = 0.0
        t2025 = float(pd.to_numeric(rep["?????? 2025"], errors="coerce").fillna(0.0).sum())
        rep = pd.concat(
            [
                rep,
                pd.DataFrame(
                    [
                        {
                            "?? ????": "????",
                            "?????? 2025": t2025,
                            "?????? 2026": t2025,
                            "???? ??? 2025 ? 2026": 0.0,
                            "????? ???????": 0.0 if t2025 > 0 else math.nan,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
        title = "??? ???? (2025?2026) — ?????? ????"
        fname = "uzeb_company_sales_2025_2026.xlsx"
        st.download_button(
            "?? ???? ??? ???? (Excel)",
            data=make_agent_sales_excel(title, rep),
            file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    else:
        agent_sales_df = build_agent_sales_report_2025_2026(context_username, scope_df, user_qty)
        title = f"??? ?????? {scope_agent_display} (2025?2026): {context_username}"
        fname = f"uzeb_{safe_filename(str(context_agent_id))}__{safe_filename(context_username)}__sales_2025_2026.xlsx"
        st.download_button(
            "?? ???? ??? ?????? (Excel)",
            data=make_agent_sales_excel(title, agent_sales_df),
            file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)

    if (not (IS_ADMIN and admin_company_wide)) and single:
        account = selected_customers[0]
        df_cust = df_scope.copy()

        pick_key = f"pick_class::{context_username}::{context_agent_id}::{account}"
        if pick_key not in st.session_state:
            st.session_state[pick_key] = ""

        class_view = build_class_view(user_qty, context_username, account, df_cust)

        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("### ????? ????? (???? ????)")
        st.info("?? ???? **????? ??? (????)**. ??? ??? ???? ??'????? ??? ????? ????? ??????.")

        form_key = f"targets_form::{context_username}::{context_agent_id}::{account}"

        with st.form(key=form_key, clear_on_submit=False):
            edited = st.data_editor(
                class_view.sort_values("??????_????", ascending=False).reset_index(drop=True),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "?? ??? ???? ????": st.column_config.TextColumn("?? ??? ????", disabled=True),
                    "??????_????": st.column_config.NumberColumn("?????? (?)", disabled=True, format="%.2f"),
                    "??????_?????": st.column_config.NumberColumn("?????? (????)", disabled=True, format="%.2f"),
                    "????_?????": st.column_config.NumberColumn("???? ?????", disabled=True, format="%.2f"),
                    "?????_???_????": st.column_config.NumberColumn("????? ??? (????)", step=1.0, format="%.2f"),
                    "?????_???_???": st.column_config.NumberColumn("????? ??? (?)", disabled=True, format="%.2f"),
                    "???_????": st.column_config.NumberColumn("2026 (?)", disabled=True, format="%.2f"),
                    "???_?????": st.column_config.NumberColumn("2026 (????)", disabled=True, format="%.2f"),
                },
                key=f"class_editor::{context_username}::{context_agent_id}::{account}",
            )
            b1, b2 = st.columns([1, 1], gap="small")
            with b1:
                refresh_clicked = st.form_submit_button("???? ?????", use_container_width=True)
            with b2:
                save_clicked = st.form_submit_button("???? ????", use_container_width=True)

        if refresh_clicked or save_clicked:
            edited["?????_???_????"] = pd.to_numeric(edited["?????_???_????"], errors="coerce").fillna(0.0)

            for _, r in edited.iterrows():
                cls = str(r["?? ??? ???? ????"])
                dq = float(r["?????_???_????"] or 0.0)
                key = (str(context_username), str(account), str(cls))
                user_qty[key] = dq
                if save_clicked:
                    db_upsert_user_qty(con, context_username, account, cls, dq)

            st.session_state[qty_key] = user_qty
            s2025_, s2026_, diff_, pct_ = compute_scope_kpi(context_username, df_scope, user_qty, [account])
            kpi_block(s2026_, s2025_, diff_, pct_, share_pct, title_2026="??????/??? 2026 (?)")
            st.success("???? ??????." if save_clicked else "?????.")

        st.markdown("#### ????? ??? ???? ????? ????? ?????? (?'?????)")
        if COL_ITEM not in df_cust.columns:
            st.caption('?? ????? ????? "?? ????" ????? — ?? ???? ????? ????? ??????.')
        else:
            pick_df = (
                class_view.sort_values("??????_????", ascending=False)
                .reset_index(drop=True)[["?? ??? ???? ????", "??????_????"]]
                .copy()
            )

            if not st.session_state[pick_key] and len(pick_df):
                st.session_state[pick_key] = str(pick_df.iloc[0]["?? ??? ???? ????"])

            pick_df.insert(0, "???", False)
            pick_df["???"] = pick_df["?? ??? ???? ????"].astype(str).apply(
                lambda x: str(x) == str(st.session_state.get(pick_key, ""))
            )

            pick_edited = st.data_editor(
                pick_df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "???": st.column_config.CheckboxColumn("???"),
                    "?? ??? ???? ????": st.column_config.TextColumn("?? ??? ????", disabled=True),
                    "??????_????": st.column_config.NumberColumn("?????? (?)", disabled=True, format="%.2f"),
                },
                key=f"{pick_key}::editor",
            )

            chosen_rows = pick_edited[pick_edited["???"] == True]
            if chosen_rows.empty and len(pick_edited):
                chosen_cls = str(pick_edited.iloc[0]["?? ??? ???? ????"])
            else:
                chosen_cls = str(chosen_rows.iloc[0]["?? ??? ???? ????"])
            st.session_state[pick_key] = chosen_cls

            st.markdown("#### ????? ?????? ??? ??? ???? ?????")
            items_df = df_cust[df_cust[COL_CLASS].astype(str) == str(chosen_cls)].copy()
            items_sum = (
                items_df.groupby([COL_CLASS, COL_ITEM], dropna=False)
                .agg(??????_????=(COL_NET, "sum"), ??????_?????=(COL_QTY, "sum"))
                .reset_index()
                .sort_values("??????_????", ascending=False)
                .reset_index(drop=True)
            )
            items_sum = items_sum.rename(columns={COL_CLASS: "??? ????", COL_ITEM: "?? ????"})
            st.dataframe(
                items_sum[["??? ????", "?? ????", "??????_????", "??????_?????"]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "??????_????": st.column_config.NumberColumn("?????? (?)", format="%.2f"),
                    "??????_?????": st.column_config.NumberColumn("????", format="%.2f"),
                },
            )

        st.markdown("</div>", unsafe_allow_html=True)

    elif (IS_ADMIN and admin_company_wide) and (single or multi):
        st.info("?????? ???? ???? ??? ????? ?????. ??? ?? '????? ???? ????' ??? ????? ????? ?????/?????.")
