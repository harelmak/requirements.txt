# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v8.8.1 (FULL FILE)

Fixes (per user report: "I don't see computed values"):
1) Force recompute + redraw after refresh/save:
   - After class targets refresh/save => st.rerun()
   - After items refresh/save => st.rerun()

2) Default column-picks for "עריכת יעדים (לקוח יחיד)" include monthly computed columns
   (only when CAN_SEE_QTY).

3) Computed columns are always recomputed from fresh views (class_view/base_df rebuilt on rerun).

Notes:
- Monthly values = yearly qty / 12
- DB stores only results snapshots (monthly_avg_2025_qty, monthly_add_qty) in both class+item tables
- Item deltas aggregate into class totals for KPI + class view
"""

import base64
import gzip
import hashlib
import hmac
import json
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
# Constants
# =========================
MONTHS_IN_YEAR = 12.0

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
# Excel Columns (source headers)
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
DB_FILENAME = "uzeb_app.sqlite"
DEFAULT_DB_DIR = Path(".") / "data"

if "db_dir" not in st.session_state:
    st.session_state["db_dir"] = str(DEFAULT_DB_DIR)


def get_db_path() -> Path:
    d = Path(str(st.session_state.get("db_dir", str(DEFAULT_DB_DIR))).strip())
    return d / DB_FILENAME


def ensure_db_dir_exists(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _sqlite_pragmas(con_: sqlite3.Connection):
    con_.execute("PRAGMA journal_mode=WAL;")
    con_.execute("PRAGMA synchronous=NORMAL;")
    con_.execute("PRAGMA temp_store=MEMORY;")
    con_.execute("PRAGMA foreign_keys=ON;")


def _try_add_column(con_: sqlite3.Connection, table: str, col_def: str):
    try:
        con_.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
    except sqlite3.OperationalError:
        pass


def ensure_all_schema(con_: sqlite3.Connection):
    _sqlite_pragmas(con_)

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

    # CLASS-level targets
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
    _try_add_column(con_, "user_class_delta_qty", "monthly_avg_2025_qty REAL NOT NULL DEFAULT 0")
    _try_add_column(con_, "user_class_delta_qty", "monthly_add_qty REAL NOT NULL DEFAULT 0")

    # ITEM-level targets
    con_.execute(
        """
        CREATE TABLE IF NOT EXISTS user_item_delta_qty (
            username TEXT NOT NULL,
            account TEXT NOT NULL,
            cls TEXT NOT NULL,
            item TEXT NOT NULL,
            delta_qty REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (username, account, cls, item),
            FOREIGN KEY(username) REFERENCES users(username)
        )
        """
    )
    _try_add_column(con_, "user_item_delta_qty", "monthly_avg_2025_qty REAL NOT NULL DEFAULT 0")
    _try_add_column(con_, "user_item_delta_qty", "monthly_add_qty REAL NOT NULL DEFAULT 0")

    con_.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            user_visible_cols_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    row = con_.execute("SELECT 1 FROM app_settings WHERE id=1").fetchone()
    if row is None:
        default_cols = [COL_AGENT, COL_ACCOUNT, COL_CLASS, COL_ITEM, COL_QTY]  # default: no money
        now = datetime.now(timezone.utc).isoformat()
        con_.execute(
            "INSERT INTO app_settings(id, user_visible_cols_json, updated_at) VALUES(1,?,?)",
            (json.dumps(default_cols, ensure_ascii=False), now),
        )

    con_.commit()


def db_connect(db_path: Path) -> sqlite3.Connection:
    ensure_db_dir_exists(db_path)
    con_ = sqlite3.connect(db_path.as_posix(), check_same_thread=False, timeout=30)
    ensure_all_schema(con_)
    return con_


@st.cache_resource
def get_db(db_path_str: str) -> sqlite3.Connection:
    return db_connect(Path(db_path_str))


db_path = get_db_path()
con = get_db(str(db_path))


def db_ready(con_: sqlite3.Connection) -> sqlite3.Connection:
    ensure_all_schema(con_)
    return con_


# =========================
# Serialization helpers
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
# App settings
# =========================
def db_load_user_visible_cols(con_) -> list[str]:
    con_ = db_ready(con_)
    row = con_.execute("SELECT user_visible_cols_json FROM app_settings WHERE id=1").fetchone()
    if not row:
        return []
    try:
        vals = json.loads(row[0])
        return [str(x) for x in vals if str(x).strip()] if isinstance(vals, list) else []
    except Exception:
        return []


def db_save_user_visible_cols(con_, cols: list[str]):
    con_ = db_ready(con_)
    now = datetime.now(timezone.utc).isoformat()
    payload = json.dumps([str(c) for c in cols], ensure_ascii=False)
    con_.execute(
        """
        INSERT INTO app_settings(id, user_visible_cols_json, updated_at)
        VALUES(1,?,?)
        ON CONFLICT(id) DO UPDATE SET
            user_visible_cols_json=excluded.user_visible_cols_json,
            updated_at=excluded.updated_at
        """,
        (payload, now),
    )
    con_.commit()


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
    con_ = db_ready(con_)
    username = str(username).strip()
    agent_id = str(agent_id).strip()
    if not username or not password or not agent_id:
        raise ValueError("username/password/agent_id required")
    if username.upper() == ADMIN_USERNAME:
        raise ValueError("ADMIN הוא שם שמור.")

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
    con_ = db_ready(con_)
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
    con_ = db_ready(con_)
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
        raise ValueError(f"חסרות עמודות בקובץ: {missing}")

    out = df.copy()
    out = out[out[COL_ACCOUNT].notna()]

    out[COL_AGENT] = out[COL_AGENT].astype(str).str.strip()
    out[COL_ACCOUNT] = out[COL_ACCOUNT].astype(str).str.strip()
    out[COL_CLASS] = out[COL_CLASS].astype(str).str.strip()

    if COL_ITEM in out.columns:
        out[COL_ITEM] = out[COL_ITEM].astype(str).str.strip()

    out[COL_QTY] = pd.to_numeric(out[COL_QTY], errors="coerce").fillna(0.0)
    out[COL_NET] = pd.to_numeric(out[COL_NET], errors="coerce").fillna(0.0)

    out[COL_AGENT] = out[COL_AGENT].astype("category")
    out[COL_ACCOUNT] = out[COL_ACCOUNT].astype("category")
    out[COL_CLASS] = out[COL_CLASS].astype("category")
    return out


# =========================
# DB: Company file + processed DF
# =========================
def db_upsert_company_file(con_, filename: str, file_bytes: bytes):
    con_ = db_ready(con_)
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
    con_ = db_ready(con_)
    row = con_.execute("SELECT filename, file_bytes, uploaded_at FROM company_sales_file WHERE id=1").fetchone()
    if not row:
        return None
    return {"filename": row[0] or "", "file_bytes": bytes(row[1]), "uploaded_at": row[2]}


def db_upsert_company_processed(con_, source_uploaded_at: str, df_norm: pd.DataFrame):
    con_ = db_ready(con_)
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
    con_ = db_ready(con_)
    row = con_.execute(
        "SELECT source_uploaded_at, df_gz_bytes, created_at, nrows FROM company_sales_processed WHERE id=1"
    ).fetchone()
    if not row:
        return None
    return {"source_uploaded_at": row[0], "df_gz_bytes": bytes(row[1]), "created_at": row[2], "nrows": int(row[3])}


# =========================
# DB: Optional personal user file (raw + processed)
# =========================
def db_upsert_user_file(con_, username: str, filename: str, file_bytes: bytes):
    con_ = db_ready(con_)
    now = datetime.now(timezone.utc).isoformat()
    con_.execute(
        """
        INSERT INTO user_sales_file(username, filename, file_bytes, uploaded_at)
        VALUES(?,?,?,?)
        ON CONFLICT(username) DO UPDATE SET
            filename=excluded.filename,
            file_bytes=excluded.file_bytes,
            uploaded_at=excluded.uploaded_at
        """,
        (str(username), str(filename or ""), sqlite3.Binary(file_bytes), now),
    )
    con_.commit()
    return now


def db_load_user_file(con_, username: str) -> Optional[dict]:
    con_ = db_ready(con_)
    row = con_.execute(
        "SELECT filename, file_bytes, uploaded_at FROM user_sales_file WHERE username=?",
        (str(username),),
    ).fetchone()
    if not row:
        return None
    return {"filename": row[0] or "", "file_bytes": bytes(row[1]), "uploaded_at": row[2]}


def db_upsert_user_processed(con_, username: str, source_uploaded_at: str, df_norm: pd.DataFrame):
    con_ = db_ready(con_)
    now = datetime.now(timezone.utc).isoformat()
    gz_bytes = df_to_gz_bytes(df_norm)
    con_.execute(
        """
        INSERT INTO user_sales_processed(username, source_uploaded_at, df_gz_bytes, created_at, nrows)
        VALUES(?,?,?,?,?)
        ON CONFLICT(username) DO UPDATE SET
            source_uploaded_at=excluded.source_uploaded_at,
            df_gz_bytes=excluded.df_gz_bytes,
            created_at=excluded.created_at,
            nrows=excluded.nrows
        """,
        (str(username), str(source_uploaded_at), sqlite3.Binary(gz_bytes), now, int(len(df_norm))),
    )
    con_.commit()


def db_load_user_processed(con_, username: str) -> Optional[dict]:
    con_ = db_ready(con_)
    row = con_.execute(
        "SELECT source_uploaded_at, df_gz_bytes, created_at, nrows FROM user_sales_processed WHERE username=?",
        (str(username),),
    ).fetchone()
    if not row:
        return None
    return {"source_uploaded_at": row[0], "df_gz_bytes": bytes(row[1]), "created_at": row[2], "nrows": int(row[3])}


# =========================
# DB: Users list + delete
# =========================
def db_list_non_admin_users(con_) -> pd.DataFrame:
    con_ = db_ready(con_)
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
    con_ = db_ready(con_)
    con_.execute("UPDATE users SET is_active=0 WHERE username=?", (str(username),))
    con_.commit()


def db_delete_user_targets(con_, username: str):
    con_ = db_ready(con_)
    con_.execute("DELETE FROM user_class_delta_qty WHERE username=?", (str(username),))
    con_.execute("DELETE FROM user_item_delta_qty WHERE username=?", (str(username),))
    con_.commit()


def db_delete_user_file(con_, username: str):
    con_ = db_ready(con_)
    con_.execute("DELETE FROM user_sales_file WHERE username=?", (str(username),))
    con_.execute("DELETE FROM user_sales_processed WHERE username=?", (str(username),))
    con_.commit()


def db_hard_delete_user(con_, username: str):
    con_ = db_ready(con_)
    con_.execute("DELETE FROM users WHERE username=?", (str(username),))
    con_.commit()


# =========================
# DB: Per-user targets (CLASS)
# =========================
def db_load_user_class_qty(con_, username: str) -> dict:
    con_ = db_ready(con_)
    rows = con_.execute(
        "SELECT account, cls, delta_qty FROM user_class_delta_qty WHERE username=?",
        (str(username),),
    ).fetchall()
    return {(str(username), str(acc), str(cls)): float(dq or 0.0) for acc, cls, dq in rows}


def db_upsert_user_class_qty(
    con_,
    username: str,
    account: str,
    cls: str,
    delta_qty: float,
    monthly_avg_2025_qty: float,
    monthly_add_qty: float,
):
    con_ = db_ready(con_)
    now = datetime.now(timezone.utc).isoformat()
    con_.execute(
        """
        INSERT INTO user_class_delta_qty(username, account, cls, delta_qty, updated_at, monthly_avg_2025_qty, monthly_add_qty)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(username, account, cls) DO UPDATE SET
            delta_qty=excluded.delta_qty,
            updated_at=excluded.updated_at,
            monthly_avg_2025_qty=excluded.monthly_avg_2025_qty,
            monthly_add_qty=excluded.monthly_add_qty
        """,
        (
            str(username),
            str(account),
            str(cls),
            float(delta_qty or 0.0),
            now,
            float(monthly_avg_2025_qty or 0.0),
            float(monthly_add_qty or 0.0),
        ),
    )
    con_.commit()


# =========================
# DB: Per-user targets (ITEM)
# =========================
def db_load_user_item_qty(con_, username: str) -> dict:
    con_ = db_ready(con_)
    rows = con_.execute(
        "SELECT account, cls, item, delta_qty FROM user_item_delta_qty WHERE username=?",
        (str(username),),
    ).fetchall()
    return {(str(username), str(acc), str(cls), str(item)): float(dq or 0.0) for acc, cls, item, dq in rows}


def db_upsert_user_item_qty(
    con_,
    username: str,
    account: str,
    cls: str,
    item: str,
    delta_qty: float,
    monthly_avg_2025_qty: float,
    monthly_add_qty: float,
):
    con_ = db_ready(con_)
    now = datetime.now(timezone.utc).isoformat()
    con_.execute(
        """
        INSERT INTO user_item_delta_qty(username, account, cls, item, delta_qty, updated_at, monthly_avg_2025_qty, monthly_add_qty)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(username, account, cls, item) DO UPDATE SET
            delta_qty=excluded.delta_qty,
            updated_at=excluded.updated_at,
            monthly_avg_2025_qty=excluded.monthly_avg_2025_qty,
            monthly_add_qty=excluded.monthly_add_qty
        """,
        (
            str(username),
            str(account),
            str(cls),
            str(item),
            float(delta_qty or 0.0),
            now,
            float(monthly_avg_2025_qty or 0.0),
            float(monthly_add_qty or 0.0),
        ),
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
        return f"₪ {float(x):,.2f}"
    except Exception:
        return "₪ 0.00"


def fmt_qty(x) -> str:
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return "0.00"


def fmt_pct(x) -> str:
    if x is None or pd.isna(x):
        return "—"
    return f"{float(x):,.1f}%"


def user_can_see_col(col_name: str, is_admin: bool, user_visible_cols: list[str]) -> bool:
    if is_admin:
        return True
    return str(col_name) in set([str(x) for x in (user_visible_cols or [])])


def user_can_see_money(is_admin: bool, user_visible_cols: list[str]) -> bool:
    return user_can_see_col(COL_NET, is_admin, user_visible_cols)


def user_can_see_qty(is_admin: bool, user_visible_cols: list[str]) -> bool:
    return user_can_see_col(COL_QTY, is_admin, user_visible_cols)


def user_can_see_item(is_admin: bool, user_visible_cols: list[str]) -> bool:
    return user_can_see_col(COL_ITEM, is_admin, user_visible_cols)


# =========================
# Core computations
# =========================
def compute_classes(df: pd.DataFrame) -> pd.DataFrame:
    g = (
        df.groupby(COL_CLASS, dropna=False)
        .agg(sales_money=(COL_NET, "sum"), sales_qty=(COL_QTY, "sum"))
        .reset_index()
        .sort_values("sales_money", ascending=False)
        .reset_index(drop=True)
    )
    g["avg_price"] = g.apply(lambda r: safe_div(r["sales_money"], r["sales_qty"]), axis=1)
    return g


def kpi_block_money(
    s2026: float,
    s2025: float,
    diff_money: float,
    pct: float,
    share_pct: Optional[float],
    title_2026: str,
):
    share_line = ""
    if share_pct is not None and not pd.isna(share_pct):
        share_line = f"<div class='sub'>נתח לקוח מהמכירות של הסוכן: {fmt_pct(share_pct)}</div>"

    st.markdown(
        f"""
        <div class="kpi-grid">
            <div class="kpi">
                <div class="label">{title_2026}</div>
                <div class="value">{fmt_money(s2026)}</div>
                {share_line}
            </div>
            <div class="kpi">
                <div class="label">מכירות 2025 (₪)</div>
                <div class="value">{fmt_money(s2025)}</div>
                <div class="sub">סכום נטו מהקובץ</div>
            </div>
            <div class="kpi">
                <div class="label">הפרש (₪)</div>
                <div class="value">{fmt_money(diff_money)}</div>
                <div class="sub">2026 - 2025</div>
            </div>
            <div class="kpi">
                <div class="label">שינוי (%)</div>
                <div class="value">{fmt_pct(pct)}</div>
                <div class="sub">(2026/2025)*100 - 100</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def kpi_block_qty(q2026: float, q2025: float, diff_qty: float, pct: float, title_2026: str):
    st.markdown(
        f"""
        <div class="kpi-grid">
            <div class="kpi">
                <div class="label">{title_2026}</div>
                <div class="value">{fmt_qty(q2026)}</div>
                <div class="sub">כמות</div>
            </div>
            <div class="kpi">
                <div class="label">כמות 2025</div>
                <div class="value">{fmt_qty(q2025)}</div>
                <div class="sub">סה״כ כמות מהקובץ</div>
            </div>
            <div class="kpi">
                <div class="label">הפרש (כמות)</div>
                <div class="value">{fmt_qty(diff_qty)}</div>
                <div class="sub">2026 - 2025</div>
            </div>
            <div class="kpi">
                <div class="label">שינוי (%)</div>
                <div class="value">{fmt_pct(pct)}</div>
                <div class="sub">(2026/2025)*100 - 100</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =========================
# Targets logic (CLASS + ITEM)
# =========================
def get_class_delta_qty(user_class_qty: dict, username: str, account: str, cls: str) -> float:
    return float(user_class_qty.get((str(username), str(account), str(cls)), 0.0) or 0.0)


def sum_item_delta_qty_for_class(user_item_qty: dict, username: str, account: str, cls: str) -> float:
    total = 0.0
    u = str(username)
    a = str(account)
    c = str(cls)
    for (uu, acc, cc, _item), dq in user_item_qty.items():
        if str(uu) == u and str(acc) == a and str(cc) == c:
            total += float(dq or 0.0)
    return total


def build_class_view(
    user_class_qty: dict,
    user_item_qty: dict,
    username: str,
    account: str,
    df_customer: pd.DataFrame,
) -> pd.DataFrame:
    class_df = compute_classes(df_customer)

    def eff_delta(cls_val: str) -> float:
        base = get_class_delta_qty(user_class_qty, username, account, cls_val)
        add_items = sum_item_delta_qty_for_class(user_item_qty, username, account, cls_val)
        return float(base + add_items)

    class_df["delta_qty"] = class_df[COL_CLASS].astype(str).apply(eff_delta)

    def qty_to_money_row(r):
        p = r["avg_price"]
        dq = float(r["delta_qty"] or 0.0)
        if pd.isna(p) or float(p) == 0:
            return 0.0
        return dq * float(p)

    class_df["delta_money"] = class_df.apply(qty_to_money_row, axis=1)
    class_df["target_money"] = class_df["sales_money"] + class_df["delta_money"]
    class_df["target_qty"] = class_df["sales_qty"] + class_df["delta_qty"]

    # monthly columns
    class_df["monthly_avg_2025_qty"] = class_df["sales_qty"].apply(lambda x: float(x or 0.0) / MONTHS_IN_YEAR)
    class_df["monthly_add_qty"] = class_df["delta_qty"].apply(lambda x: float(x or 0.0) / MONTHS_IN_YEAR)
    class_df["monthly_target_2026_qty"] = class_df["target_qty"].apply(lambda x: float(x or 0.0) / MONTHS_IN_YEAR)

    out = class_df[
        [
            COL_CLASS,
            "sales_money",
            "sales_qty",
            "avg_price",
            "delta_money",
            "delta_qty",
            "target_money",
            "target_qty",
            "monthly_avg_2025_qty",
            "monthly_add_qty",
            "monthly_target_2026_qty",
        ]
    ].copy()

    out = out.rename(
        columns={
            COL_CLASS: "שם קוד מיון פריט",
            "sales_money": "מכירות_בכסף",
            "sales_qty": "מכירות_בכמות",
            "avg_price": "מחיר_ממוצע",
            "delta_money": "תוספת_יעד_כסף",
            "delta_qty": "תוספת_יעד_כמות",
            "target_money": "יעד_בכסף",
            "target_qty": "יעד_בכמות",
            "monthly_avg_2025_qty": "ממוצע_חודשי_כמות_2025",
            "monthly_add_qty": "תוספת_חודשית_כמות",
            "monthly_target_2026_qty": "יעד_חודשי_כמות_2026",
        }
    )
    return out


def _allowed_accounts(df_scope: pd.DataFrame, selected_accounts: Optional[list[str]]) -> set:
    scope_accounts = set(df_scope[COL_ACCOUNT].dropna().astype(str).tolist())
    if selected_accounts is None:
        return scope_accounts
    return set([str(x) for x in selected_accounts]) & scope_accounts


def compute_scope_kpi_money(
    username: str,
    df_scope: pd.DataFrame,
    user_class_qty: dict,
    user_item_qty: dict,
    selected_accounts: Optional[list[str]],
):
    class_sales = compute_classes(df_scope)
    allowed = _allowed_accounts(df_scope, selected_accounts)

    def agg_eff_delta_qty(cls_val: str) -> float:
        total = 0.0
        u = str(username)
        c = str(cls_val)
        for (uu, acc, cc), dq in user_class_qty.items():
            if str(uu) != u or str(acc) not in allowed:
                continue
            if str(cc) == c:
                total += float(dq or 0.0)
        for (uu, acc, cc, _item), dq in user_item_qty.items():
            if str(uu) != u or str(acc) not in allowed:
                continue
            if str(cc) == c:
                total += float(dq or 0.0)
        return total

    class_sales["delta_qty"] = class_sales[COL_CLASS].astype(str).apply(agg_eff_delta_qty)

    def qty_to_money_row(r):
        p = r["avg_price"]
        dq = float(r["delta_qty"] or 0.0)
        if pd.isna(p) or float(p) == 0:
            return 0.0
        return dq * float(p)

    class_sales["delta_money"] = class_sales.apply(qty_to_money_row, axis=1)

    s2025 = float(pd.to_numeric(class_sales["sales_money"], errors="coerce").fillna(0.0).sum())
    add_money = float(pd.to_numeric(class_sales["delta_money"], errors="coerce").fillna(0.0).sum())
    s2026 = s2025 + add_money
    diff = s2026 - s2025
    pct = (safe_div(s2026, s2025) * 100 - 100) if s2025 > 0 else math.nan
    return s2025, s2026, diff, pct


def compute_scope_kpi_qty(
    username: str,
    df_scope: pd.DataFrame,
    user_class_qty: dict,
    user_item_qty: dict,
    selected_accounts: Optional[list[str]],
):
    class_sales = compute_classes(df_scope)
    allowed = _allowed_accounts(df_scope, selected_accounts)

    def agg_eff_delta_qty(cls_val: str) -> float:
        total = 0.0
        u = str(username)
        c = str(cls_val)
        for (uu, acc, cc), dq in user_class_qty.items():
            if str(uu) != u or str(acc) not in allowed:
                continue
            if str(cc) == c:
                total += float(dq or 0.0)
        for (uu, acc, cc, _item), dq in user_item_qty.items():
            if str(uu) != u or str(acc) not in allowed:
                continue
            if str(cc) == c:
                total += float(dq or 0.0)
        return total

    class_sales["delta_qty"] = class_sales[COL_CLASS].astype(str).apply(agg_eff_delta_qty)

    q2025 = float(pd.to_numeric(class_sales["sales_qty"], errors="coerce").fillna(0.0).sum())
    add_qty = float(pd.to_numeric(class_sales["delta_qty"], errors="coerce").fillna(0.0).sum())
    q2026 = q2025 + add_qty
    diff = q2026 - q2025
    pct = (safe_div(q2026, q2025) * 100 - 100) if q2025 > 0 else math.nan
    return q2025, q2026, diff, pct


# =========================
# Excel report
# =========================
def build_agent_sales_report_2025_2026(
    username: str,
    agent_df: pd.DataFrame,
    user_class_qty: dict,
    user_item_qty: dict,
) -> pd.DataFrame:
    customers = agent_df[COL_ACCOUNT].dropna().astype(str).unique().tolist()
    rows = []
    for acc in customers:
        df_c = agent_df[agent_df[COL_ACCOUNT].astype(str) == str(acc)].copy()
        if df_c.empty:
            continue
        class_view = build_class_view(user_class_qty, user_item_qty, username, str(acc), df_c)
        s2025 = float(pd.to_numeric(class_view["מכירות_בכסף"], errors="coerce").fillna(0.0).sum())
        add_money = float(pd.to_numeric(class_view["תוספת_יעד_כסף"], errors="coerce").fillna(0.0).sum())
        s2026 = s2025 + add_money
        diff = s2026 - s2025
        pct = (safe_div(s2026, s2025) * 100 - 100) if s2025 > 0 else math.nan

        rows.append(
            {
                "שם לקוח": str(acc),
                "מכירות 2025": s2025,
                "מכירות 2026": s2026,
                "הפרש בין 2025 ל 2026": diff,
                "שינוי באחוזים": pct,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values("מכירות 2025", ascending=False).reset_index(drop=True)

    t2025 = float(pd.to_numeric(df["מכירות 2025"], errors="coerce").fillna(0.0).sum())
    t2026 = float(pd.to_numeric(df["מכירות 2026"], errors="coerce").fillna(0.0).sum())
    tdiff = t2026 - t2025
    tpct = (safe_div(t2026, t2025) * 100 - 100) if t2025 > 0 else math.nan

    df_total = pd.DataFrame(
        [
            {
                "שם לקוח": "סה״כ",
                "מכירות 2025": t2025,
                "מכירות 2026": t2026,
                "הפרש בין 2025 ל 2026": tdiff,
                "שינוי באחוזים": tpct,
            }
        ]
    )
    return pd.concat([df, df_total], ignore_index=True)


def make_agent_sales_excel(title: str, df_report: pd.DataFrame) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "דוח"
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
    cols = ["שם לקוח", "מכירות 2025", "מכירות 2026", "הפרש בין 2025 ל 2026", "שינוי באחוזים"]

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
        is_total = (str(row[0]).strip() == "סה״כ")
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
# FAST LOAD: processed DF (self-heal)
# =========================
@st.cache_data(show_spinner=False)
def load_company_sales_df_cached(source_uploaded_at: str, gz_bytes: bytes) -> pd.DataFrame:
    return df_from_gz_bytes(gz_bytes)


def get_company_sales_df(con_) -> pd.DataFrame:
    con_ = db_ready(con_)
    company = db_load_company_file(con_)
    if company is None:
        raise ValueError("אין קובץ חברה במערכת.")

    proc = db_load_company_processed(con_)
    if proc is not None and str(proc["source_uploaded_at"]) == str(company["uploaded_at"]):
        return load_company_sales_df_cached(proc["source_uploaded_at"], proc["df_gz_bytes"])

    df_raw = read_sales_excel_bytes(company["file_bytes"])
    df_norm = normalize_sales_strict(df_raw)
    db_upsert_company_processed(con_, company["uploaded_at"], df_norm)
    proc2 = db_load_company_processed(con_)
    return load_company_sales_df_cached(proc2["source_uploaded_at"], proc2["df_gz_bytes"])


@st.cache_data(show_spinner=False)
def load_user_sales_df_cached(username: str, source_uploaded_at: str, gz_bytes: bytes) -> pd.DataFrame:
    return df_from_gz_bytes(gz_bytes)


def get_user_sales_df(con_, username: str) -> Optional[pd.DataFrame]:
    con_ = db_ready(con_)
    raw = db_load_user_file(con_, username)
    if raw is None:
        return None

    proc = db_load_user_processed(con_, username)
    if proc is not None and str(proc["source_uploaded_at"]) == str(raw["uploaded_at"]):
        return load_user_sales_df_cached(username, proc["source_uploaded_at"], proc["df_gz_bytes"])

    df_raw = read_sales_excel_bytes(raw["file_bytes"])
    df_norm = normalize_sales_strict(df_raw)
    db_upsert_user_processed(con_, username, raw["uploaded_at"], df_norm)
    proc2 = db_load_user_processed(con_, username)
    return load_user_sales_df_cached(username, proc2["source_uploaded_at"], proc2["df_gz_bytes"])


# =========================
# Header
# =========================
st.markdown(
    """
<div class="card">
  <h2>📊 Uzeb — ניהול יעדי מכירות</h2>
  <p>קובץ מרכזי אחד לכל החברה. סוכן רואה רק את שלו. ADMIN יכול לראות הכל ולסנן לפי סוכן/משתמש.</p>
</div>
""",
    unsafe_allow_html=True,
)

# =========================
# Sidebar: DB Path
# =========================
with st.sidebar:
    st.markdown("### שמירה (SQLite)")
    st.text_input("נתיב תיקייה למסד נתונים", key="db_dir")
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
# Sidebar: Login
# =========================
with st.sidebar:
    st.markdown("---")
    st.markdown("### כניסה")

    if st.session_state.get("logged_in") != True:
        users_df = db_list_non_admin_users(con)
        if not users_df.empty:
            users_df = users_df[users_df["is_active"] == 1].copy()

        usernames = []
        if users_df is not None and not users_df.empty:
            usernames = sorted(users_df["username"].astype(str).unique().tolist(), key=lambda x: x.lower())

        login_options = [ADMIN_USERNAME] + usernames
        u_in = st.selectbox("שם משתמש", options=login_options, index=0, key="login_user")
        p_in = st.text_input("סיסמה", type="password", key="login_pass")

        if st.button("כניסה", use_container_width=True):
            ok, u = verify_login(con, u_in, p_in)
            if not ok:
                st.error("שם משתמש/סיסמה לא תקינים או משתמש לא פעיל.")
                st.stop()
            st.session_state["logged_in"] = True
            st.session_state["login_username"] = str(u["username"])
            st.session_state["is_admin"] = bool(u.get("is_admin", False))
            st.session_state["agent_id"] = str(u.get("agent_id", "") or "").strip()
            st.session_state["agent_name"] = str(u.get("agent_name", "") or "").strip()
            st.rerun()
    else:
        is_admin = bool(st.session_state.get("is_admin", False))
        st.success(f"מחובר: {st.session_state.get('login_username')}" + (" (ADMIN)" if is_admin else ""))
        if not is_admin:
            st.caption(f"סוכן: {agent_label(st.session_state.get('agent_id'))}")
        if st.button("יציאה", use_container_width=True):
            st.session_state.clear()
            st.rerun()

# =========================
# Require login
# =========================
if st.session_state.get("logged_in") != True:
    st.info("⬅️ יש להתחבר מהצד כדי להתחיל.")
    st.stop()

IS_ADMIN = bool(st.session_state.get("is_admin", False))

# Load permissions
USER_VISIBLE_COLS = db_load_user_visible_cols(con)
CAN_SEE_MONEY = user_can_see_money(IS_ADMIN, USER_VISIBLE_COLS)
CAN_SEE_QTY = user_can_see_qty(IS_ADMIN, USER_VISIBLE_COLS)
CAN_SEE_ITEM = user_can_see_item(IS_ADMIN, USER_VISIBLE_COLS)

# =========================
# Admin view mode (company-wide)
# =========================
admin_company_wide = False
if IS_ADMIN:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### ADMIN — מצב תצוגה")
    admin_company_wide = st.checkbox("תצוגת חברה מלאה (כל הסוכנים)", value=False)
    st.caption("בתצוגת חברה מלאה: אין חיבור ליעדי משתמשים (targets) — זו תצוגת מכירות בלבד.")
    st.markdown("</div>", unsafe_allow_html=True)

# =========================
# Sidebar: ADMIN actions
# =========================
with st.sidebar:
    if IS_ADMIN:
        st.markdown("---")
        st.markdown("### ADMIN — קובץ חברה (מרכזי)")

        company_saved = db_load_company_file(con)
        if company_saved is not None:
            st.caption(
                f"קובץ חברה שמור: {company_saved['filename'] or 'company.xlsx'} | עודכן: {company_saved['uploaded_at']}"
            )
            proc = db_load_company_processed(con)
            if proc is not None and proc["source_uploaded_at"] == company_saved["uploaded_at"]:
                st.caption(f"✅ נתונים מעובדים שמורים (n={proc['nrows']}) | נוצר: {proc['created_at']}")
            else:
                st.caption("⚠️ אין נתונים מעובדים תואמים (ייווצר אוטומטית).")
        else:
            st.caption("אין קובץ חברה שמור עדיין.")

        up_company = st.file_uploader("העלה/החלף קובץ חברה (.xlsx)", type=["xlsx"], key="company_uploader")
        if up_company is not None:
            try:
                uploaded_at = db_upsert_company_file(con, up_company.name, up_company.getvalue())
                df_raw = read_sales_excel_bytes(up_company.getvalue())
                df_norm = normalize_sales_strict(df_raw)
                db_upsert_company_processed(con, uploaded_at, df_norm)
                st.success("קובץ חברה נשמר + עיבוד נתונים בוצע.")
                st.rerun()
            except Exception as e:
                st.error(f"שגיאה בעיבוד/שמירה: {e}")
                st.stop()

        st.markdown("---")
        st.markdown("### ADMIN — ניהול תצוגה למשתמשים (לפי סוג משתמש)")

        available_cols = [COL_AGENT, COL_ACCOUNT, COL_CLASS, COL_QTY, COL_NET, COL_ITEM]
        try:
            proc = db_load_company_processed(con)
            if proc is not None:
                df_tmp = load_company_sales_df_cached(proc["source_uploaded_at"], proc["df_gz_bytes"])
                available_cols = sorted([str(c) for c in df_tmp.columns])
        except Exception:
            pass

        min_display = [COL_ACCOUNT, COL_CLASS]
        current = [c for c in (USER_VISIBLE_COLS or []) if c in available_cols]

        picked = st.multiselect(
            "בחר עמודות מתוך קובץ הנתונים שמותר למשתמשים רגילים לראות",
            options=available_cols,
            default=current,
            key="admin_visible_cols_pick",
        )

        for c in min_display:
            if c in available_cols and c not in picked:
                picked.append(c)

        if st.button("שמור הגדרות תצוגה", use_container_width=True, key="admin_save_visible_cols"):
            db_save_user_visible_cols(con, picked)
            st.success("נשמר.")
            st.rerun()

        st.markdown("---")
        st.markdown("### ADMIN — יצירת משתמשים")
        new_u = st.text_input("משתמש חדש", key="admin_new_u")
        new_p = st.text_input("סיסמה חדשה", type="password", key="admin_new_p")
        new_agent = st.text_input("מספר סוכן (agent_id)", key="admin_new_agent")
        new_agent_name = st.text_input("שם סוכן (אופציונלי)", key="admin_new_agent_name")
        if st.button("צור משתמש", use_container_width=True, key="admin_create_user"):
            try:
                create_user(con, new_u, new_p, new_agent, new_agent_name)
                st.success("נוצר משתמש.")
                st.rerun()
            except sqlite3.IntegrityError:
                st.error("שם משתמש כבר קיים.")
            except Exception as e:
                st.error(f"שגיאה: {e}")

# =========================
# Resolve context user
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
            st.error("אין משתמשים פעילים במערכת (מלבד ADMIN).")
            st.stop()

        agent_ids = sorted(users_df["agent_id"].astype(str).unique().tolist(), key=lambda x: str(x))
        if "admin_agent_filter" not in st.session_state:
            st.session_state["admin_agent_filter"] = agent_ids[0]

        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("### ADMIN — סינון")
        chosen_agent_id = st.selectbox(
            "בחר סוכן",
            options=agent_ids,
            index=agent_ids.index(st.session_state["admin_agent_filter"])
            if st.session_state["admin_agent_filter"] in agent_ids
            else 0,
            format_func=agent_label,
        )
        st.session_state["admin_agent_filter"] = chosen_agent_id

        cand = users_df[users_df["agent_id"].astype(str) == str(chosen_agent_id)].copy()
        cand["label"] = cand.apply(
            lambda r: f"{r['username']} | {agent_label(r['agent_id'])}"
            + (f" | {r['agent_name']}" if r["agent_name"] else ""),
            axis=1,
        )
        labels = cand["label"].tolist()
        label_to_user = {cand.iloc[i]["label"]: cand.iloc[i]["username"] for i in range(len(cand))}

        if "admin_user_pick" not in st.session_state or st.session_state["admin_user_pick"] not in cand["username"].tolist():
            st.session_state["admin_user_pick"] = cand.iloc[0]["username"]

        current_label = cand[cand["username"] == st.session_state["admin_user_pick"]]["label"].iloc[0]
        chosen_label = st.selectbox("בחר משתמש", options=labels, index=labels.index(current_label))
        context_username = str(label_to_user[chosen_label])
        context_agent_id = str(chosen_agent_id).strip()
        st.caption(f"מציג נתונים עבור: {context_username} | {agent_label(context_agent_id)}")
        st.markdown("</div>", unsafe_allow_html=True)

# =========================
# Load per-user targets (CLASS + ITEM)
# =========================
user_class_qty = {}
user_item_qty = {}

class_key = f"user_class_qty::{context_username}"
item_key = f"user_item_qty::{context_username}"

if not (IS_ADMIN and admin_company_wide):
    if class_key not in st.session_state:
        st.session_state[class_key] = db_load_user_class_qty(con, context_username)
    if item_key not in st.session_state:
        st.session_state[item_key] = db_load_user_item_qty(con, context_username)

    user_class_qty = st.session_state[class_key]
    user_item_qty = st.session_state[item_key]

# =========================
# DATA SOURCE
# =========================
company_saved = db_load_company_file(con)
if company_saved is None:
    st.error("אין קובץ חברה במערכת. ADMIN חייב להעלות קובץ חברה מרכזי.")
    st.stop()

data_source = "company"
personal_df = None
if not (IS_ADMIN and admin_company_wide):
    personal_df = get_user_sales_df(con, context_username)

if (not IS_ADMIN) and (personal_df is not None):
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### מקור נתונים")
    data_source = st.radio(
        "בחר מקור נתונים",
        options=["קובץ חברה (מומלץ)", "קובץ אישי שלי (אופציונלי)"],
        index=0,
        horizontal=True,
        key=f"data_source::{context_username}",
    )
    data_source = "company" if "חברה" in data_source else "personal"
    st.markdown("</div>", unsafe_allow_html=True)

with st.spinner("טוען נתונים..."):
    company_df = get_company_sales_df(con)

sales_all = company_df if data_source == "company" else personal_df
if sales_all is None:
    st.error("אין נתונים לטעינה.")
    st.stop()

# =========================
# Scope data
# =========================
if IS_ADMIN and admin_company_wide:
    scope_df = sales_all.copy()
    scope_title = "חברה מלאה (כל הסוכנים)"
    scope_agent_display = "כל החברה"
else:
    if not context_agent_id:
        st.error("למשתמש אין agent_id. ADMIN צריך לעדכן/ליצור משתמש עם agent_id.")
        st.stop()
    scope_df = sales_all[sales_all[COL_AGENT].astype(str) == str(context_agent_id)].copy()
    if scope_df.empty:
        st.error(f"לא נמצאו רשומות לסוכן {agent_label(context_agent_id)} בקובץ הנתונים.")
        st.stop()
    scope_title = f"סוכן: {agent_label(context_agent_id)}"
    scope_agent_display = agent_label(context_agent_id)

# =========================
# KPI + Tables
# =========================
st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown(f"### נתונים — {scope_title}")
if not IS_ADMIN:
    st.caption("המערכת מציגה רק את המכירות של הסוכן המחובר (סינון לפי עמודת 'סוכן בחשבון').")
st.markdown("</div>", unsafe_allow_html=True)

scope_total_money_2025 = float(pd.to_numeric(scope_df[COL_NET], errors="coerce").fillna(0.0).sum())

cust_table = (
    scope_df.groupby(COL_ACCOUNT)
    .agg(sum_money=(COL_NET, "sum"), sum_qty=(COL_QTY, "sum"))
    .reset_index()
    .sort_values("sum_money", ascending=False)
    .reset_index(drop=True)
)

cust_table["share_pct"] = cust_table["sum_money"].apply(
    lambda x: safe_div(float(x), scope_total_money_2025) * 100 if scope_total_money_2025 > 0 else math.nan
)

customer_options = cust_table[COL_ACCOUNT].astype(str).tolist()


def customer_format(acc: str) -> str:
    row = cust_table[cust_table[COL_ACCOUNT].astype(str) == str(acc)]
    if row.empty:
        return str(acc)
    p = float(row["share_pct"].iloc[0]) if "share_pct" in row.columns else math.nan
    return f"{acc} — {fmt_pct(p)}"


sel_key = f"cust_selection::{context_username}::{context_agent_id}::{admin_company_wide}"
if sel_key not in st.session_state:
    st.session_state[sel_key] = []

left, right = st.columns([1, 2], gap="large")

with left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### בחירת לקוחות (אופציונלי)")
    st.caption("ברירת מחדל: לא נבחר לקוח → KPI מוצג עבור כל הטווח. לעריכת יעדים בחר לקוח יחיד.")
    st.session_state[sel_key] = st.multiselect(
        "לקוחות (מסודר לפי מכירות) — עם נתח ליד השם",
        options=customer_options,
        default=st.session_state.get(sel_key, []),
        format_func=customer_format,
        key=f"ms_customers::{context_username}::{context_agent_id}::{admin_company_wide}",
    )

    st.markdown("#### טבלת לקוחות — 2025")

    cust_table_disp = cust_table.rename(
        columns={
            COL_ACCOUNT: "שם לקוח",
            "sum_money": "סהכ_כסף",
            "sum_qty": "סהכ_כמות",
            "share_pct": "נתח (%)",
        }
    )

    if IS_ADMIN:
        cols = ["שם לקוח", "סהכ_כסף", "סהכ_כמות", "נתח (%)"]
        cfg = {
            "שם לקוח": st.column_config.TextColumn("שם לקוח"),
            "סהכ_כסף": st.column_config.NumberColumn("מכירות 2025 (₪)", format="%.2f"),
            "סהכ_כמות": st.column_config.NumberColumn("כמות", format="%.2f"),
            "נתח (%)": st.column_config.NumberColumn("נתח (%)", format="%.1f"),
        }
    else:
        cols = ["שם לקוח", "סהכ_כמות", "נתח (%)"]
        cfg = {
            "שם לקוח": st.column_config.TextColumn("שם לקוח"),
            "סהכ_כמות": st.column_config.NumberColumn("כמות", format="%.2f"),
            "נתח (%)": st.column_config.NumberColumn("נתח (%)", format="%.1f"),
        }

    st.dataframe(cust_table_disp[cols], use_container_width=True, hide_index=True, column_config=cfg)
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    selected_customers = [str(x) for x in st.session_state.get(sel_key, [])]
    none_selected = len(selected_customers) == 0
    single = len(selected_customers) == 1

    if none_selected:
        df_scope = scope_df.copy()
        scope_subtitle = "כל הלקוחות"
        share_pct = None
        selected_accounts_for_scope = None
    elif single:
        df_scope = scope_df[scope_df[COL_ACCOUNT].astype(str) == str(selected_customers[0])].copy()
        scope_subtitle = f"לקוח: {selected_customers[0]}"
        cust_sales_money_2025 = float(pd.to_numeric(df_scope[COL_NET], errors="coerce").fillna(0.0).sum())
        share_pct = safe_div(cust_sales_money_2025, scope_total_money_2025) * 100 if scope_total_money_2025 > 0 else math.nan
        selected_accounts_for_scope = [str(selected_customers[0])]
    else:
        df_scope = scope_df[scope_df[COL_ACCOUNT].astype(str).isin(selected_customers)].copy()
        scope_subtitle = f"{len(selected_customers)} לקוחות (מסונן)"
        share_pct = None
        selected_accounts_for_scope = [str(x) for x in selected_customers]

    if df_scope.empty:
        st.error("לא נמצאו נתונים בתצוגה הנבחרת.")
        st.stop()

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### תצוגה")
    st.caption(scope_subtitle)
    st.markdown("</div>", unsafe_allow_html=True)

    if IS_ADMIN and admin_company_wide:
        if CAN_SEE_MONEY:
            s2025 = float(pd.to_numeric(df_scope[COL_NET], errors="coerce").fillna(0.0).sum())
            kpi_block_money(
                s2025,
                s2025,
                0.0,
                0.0 if s2025 > 0 else math.nan,
                share_pct if single else None,
                "מכירות 2026 (₪) — ללא יעדים",
            )
        else:
            q2025 = float(pd.to_numeric(df_scope[COL_QTY], errors="coerce").fillna(0.0).sum())
            kpi_block_qty(q2025, q2025, 0.0, 0.0 if q2025 > 0 else math.nan, "כמות 2026 — ללא יעדים")
    else:
        if CAN_SEE_MONEY:
            s2025, s2026, diff, pct = compute_scope_kpi_money(
                context_username, df_scope, user_class_qty, user_item_qty, selected_accounts_for_scope
            )
            kpi_block_money(s2026, s2025, diff, pct, share_pct if single else None, "מכירות/יעד 2026 (₪)")
        else:
            q2025, q2026, diff, pct = compute_scope_kpi_qty(
                context_username, df_scope, user_class_qty, user_item_qty, selected_accounts_for_scope
            )
            kpi_block_qty(q2026, q2025, diff, pct, "כמות/יעד 2026")

    # Excel download
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown(f"### דוח אקסל — {scope_agent_display}")

    if (not IS_ADMIN) and (not CAN_SEE_MONEY):
        st.info("הורדת דוח Excel חסומה למשתמשים רגילים כאשר נתוני כסף אינם מורשים לתצוגה.")
    else:
        st.caption("שם לקוח | מכירות 2025 | מכירות 2026 | הפרש | שינוי %")
        if IS_ADMIN and admin_company_wide:
            rep = (
                df_scope.groupby(COL_ACCOUNT)
                .agg(sales_2025=(COL_NET, "sum"))
                .reset_index()
                .rename(columns={COL_ACCOUNT: "שם לקוח", "sales_2025": "מכירות 2025"})
                .sort_values("מכירות 2025", ascending=False)
                .reset_index(drop=True)
            )
            rep["מכירות 2026"] = rep["מכירות 2025"]
            rep["הפרש בין 2025 ל 2026"] = 0.0
            rep["שינוי באחוזים"] = 0.0
            t2025 = float(pd.to_numeric(rep["מכירות 2025"], errors="coerce").fillna(0.0).sum())
            rep = pd.concat(
                [
                    rep,
                    pd.DataFrame(
                        [{
                            "שם לקוח": "סה״כ",
                            "מכירות 2025": t2025,
                            "מכירות 2026": t2025,
                            "הפרש בין 2025 ל 2026": 0.0,
                            "שינוי באחוזים": 0.0 if t2025 > 0 else math.nan,
                        }]
                    ),
                ],
                ignore_index=True,
            )

            st.download_button(
                "⬇️ הורד דוח חברה (Excel)",
                data=make_agent_sales_excel("דוח חברה (2025→2026) — מכירות בלבד", rep),
                file_name="uzeb_company_sales_2025_2026.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        else:
            agent_sales_df = build_agent_sales_report_2025_2026(context_username, scope_df, user_class_qty, user_item_qty)
            fname = f"uzeb_{safe_filename(str(context_agent_id))}__{safe_filename(context_username)}__sales_2025_2026.xlsx"
            st.download_button(
                "⬇️ הורד דוח מכירות (Excel)",
                data=make_agent_sales_excel(
                    f"דוח מכירות {scope_agent_display} (2025→2026): {context_username}", agent_sales_df
                ),
                file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
    st.markdown("</div>", unsafe_allow_html=True)

    # =========================
    # Single-customer editing (targets) + EDITABLE ITEMS
    # =========================
    if (not (IS_ADMIN and admin_company_wide)) and single:
        account = selected_customers[0]
        df_cust = df_scope.copy()

        class_view = build_class_view(user_class_qty, user_item_qty, context_username, account, df_cust)

        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("### עריכת יעדים (לקוח יחיד)")
        st.info("✏️ ערוך **תוספת יעד (כמות)**. שאר השדות מחושבים אוטומטית. (תוספות פריטים מצטרפות לקוד המיון)")

        base_df = class_view.sort_values("מכירות_בכסף", ascending=False).reset_index(drop=True)

        # ---- Column filter (per user) ----
        all_possible_cols = [
            "שם קוד מיון פריט",
            "מכירות_בכסף",
            "מכירות_בכמות",
            "מחיר_ממוצע",
            "ממוצע_חודשי_כמות_2025",
            "תוספת_יעד_כמות",
            "תוספת_חודשית_כמות",
            "תוספת_יעד_כסף",
            "יעד_בכסף",
            "יעד_בכמות",
            "יעד_חודשי_כמות_2026",
        ]

        allowed_cols = ["שם קוד מיון פריט"]
        if CAN_SEE_MONEY:
            allowed_cols += ["מכירות_בכסף", "מחיר_ממוצע", "תוספת_יעד_כסף", "יעד_בכסף"]
        if CAN_SEE_QTY:
            allowed_cols += [
                "מכירות_בכמות",
                "ממוצע_חודשי_כמות_2025",
                "תוספת_יעד_כמות",
                "תוספת_חודשית_כמות",
                "יעד_בכמות",
                "יעד_חודשי_כמות_2026",
            ]
        allowed_cols = [c for c in all_possible_cols if c in set(allowed_cols)]

        cols_pref_key = f"class_editor_cols::{context_username}::{context_agent_id}::{account}"
        if cols_pref_key not in st.session_state:
            # Default includes computed monthly columns if possible
            if CAN_SEE_MONEY and CAN_SEE_QTY:
                default_cols = [
                    "שם קוד מיון פריט",
                    "מכירות_בכסף",
                    "מכירות_בכמות",
                    "ממוצע_חודשי_כמות_2025",
                    "מחיר_ממוצע",
                    "תוספת_יעד_כמות",
                    "תוספת_חודשית_כמות",
                    "תוספת_יעד_כסף",
                    "יעד_בכסף",
                    "יעד_בכמות",
                    "יעד_חודשי_כמות_2026",
                ]
            elif CAN_SEE_QTY:
                default_cols = [
                    "שם קוד מיון פריט",
                    "מכירות_בכמות",
                    "ממוצע_חודשי_כמות_2025",
                    "תוספת_יעד_כמות",
                    "תוספת_חודשית_כמות",
                    "יעד_בכמות",
                    "יעד_חודשי_כמות_2026",
                ]
            else:
                default_cols = ["שם קוד מיון פריט", "תוספת_יעד_כמות"]
            st.session_state[cols_pref_key] = [c for c in default_cols if c in allowed_cols]

        st.markdown("#### סינון עמודות לתצוגה")
        picked_cols = st.multiselect(
            "עמודות להצגה בטבלת עריכת יעדים (לקוח יחיד)",
            options=allowed_cols,
            default=st.session_state[cols_pref_key],
            key=f"ms_{cols_pref_key}",
        )

        must_cols = ["שם קוד מיון פריט", "תוספת_יעד_כמות"]
        for c in must_cols:
            if c in allowed_cols and c not in picked_cols:
                picked_cols.append(c)

        st.session_state[cols_pref_key] = [c for c in allowed_cols if c in set(picked_cols)]

        editor_df_full = base_df[st.session_state[cols_pref_key]].copy()

        column_config = {"שם קוד מיון פריט": st.column_config.TextColumn("שם קוד מיון", disabled=True)}
        if "מכירות_בכסף" in editor_df_full.columns:
            column_config["מכירות_בכסף"] = st.column_config.NumberColumn("מכירות (₪)", disabled=True, format="%.2f")
        if "מכירות_בכמות" in editor_df_full.columns:
            column_config["מכירות_בכמות"] = st.column_config.NumberColumn("כמות 2025", disabled=True, format="%.2f")
        if "מחיר_ממוצע" in editor_df_full.columns:
            column_config["מחיר_ממוצע"] = st.column_config.NumberColumn("מחיר ממוצע", disabled=True, format="%.2f")
        if "ממוצע_חודשי_כמות_2025" in editor_df_full.columns:
            column_config["ממוצע_חודשי_כמות_2025"] = st.column_config.NumberColumn(
                "ממוצע חודשי (כמות) 2025", disabled=True, format="%.2f"
            )

        column_config["תוספת_יעד_כמות"] = st.column_config.NumberColumn("תוספת יעד (כמות)", step=1.0, format="%.2f")

        if "תוספת_חודשית_כמות" in editor_df_full.columns:
            column_config["תוספת_חודשית_כמות"] = st.column_config.NumberColumn(
                "תוספת חודשית (כמות)", disabled=True, format="%.2f"
            )
        if "תוספת_יעד_כסף" in editor_df_full.columns:
            column_config["תוספת_יעד_כסף"] = st.column_config.NumberColumn("תוספת יעד (₪)", disabled=True, format="%.2f")
        if "יעד_בכסף" in editor_df_full.columns:
            column_config["יעד_בכסף"] = st.column_config.NumberColumn("2026 (₪)", disabled=True, format="%.2f")
        if "יעד_בכמות" in editor_df_full.columns:
            column_config["יעד_בכמות"] = st.column_config.NumberColumn("כמות 2026", disabled=True, format="%.2f")
        if "יעד_חודשי_כמות_2026" in editor_df_full.columns:
            column_config["יעד_חודשי_כמות_2026"] = st.column_config.NumberColumn(
                "יעד חודשי 2026 (כמות)", disabled=True, format="%.2f"
            )

        with st.form(key=f"targets_form::{context_username}::{context_agent_id}::{account}", clear_on_submit=False):
            edited = st.data_editor(
                editor_df_full,
                hide_index=True,
                use_container_width=True,
                column_config=column_config,
                key=f"class_editor::{context_username}::{context_agent_id}::{account}",
            )
            b1, b2 = st.columns([1, 1], gap="small")
            with b1:
                refresh_clicked = st.form_submit_button("רענן חישוב", use_container_width=True)
            with b2:
                save_clicked = st.form_submit_button("שמור למסד", use_container_width=True)

        if refresh_clicked or save_clicked:
            sales_qty_map = dict(
                zip(
                    base_df["שם קוד מיון פריט"].astype(str),
                    pd.to_numeric(base_df.get("מכירות_בכמות", 0.0), errors="coerce").fillna(0.0),
                )
            )

            edited["תוספת_יעד_כמות"] = pd.to_numeric(edited["תוספת_יעד_כמות"], errors="coerce").fillna(0.0)

            for _, r in edited.iterrows():
                cls = str(r["שם קוד מיון פריט"])
                dq = float(r["תוספת_יעד_כמות"] or 0.0)

                user_class_qty[(str(context_username), str(account), str(cls))] = dq

                if save_clicked:
                    monthly_avg_2025_qty = float(sales_qty_map.get(cls, 0.0) or 0.0) / MONTHS_IN_YEAR
                    monthly_add_qty = float(dq) / MONTHS_IN_YEAR
                    db_upsert_user_class_qty(con, context_username, account, cls, dq, monthly_avg_2025_qty, monthly_add_qty)

            st.session_state[class_key] = user_class_qty

            st.success("נשמר ועודכן." if save_clicked else "עודכן.")
            # FIX: force rebuild to show computed columns
            st.rerun()

        # ========= EDITABLE ITEMS =========
        st.markdown("---")
        st.markdown("### פירוט פריטים (עריכה) — סינון לפי קוד מיון + חיפוש בשם פריט")
        st.caption("עריכת תוספת יעד לפי פריט מצטרפת לקוד המיון ומעדכנת KPI אוטומטית.")

        if COL_ITEM not in df_cust.columns:
            st.caption('לא נמצאה עמודה "שם פריט" בקובץ — לא ניתן להציג פירוט פריטים.')
        elif not CAN_SEE_ITEM:
            st.caption('לפי הרשאות התצוגה, למשתמש רגיל אין גישה לעמודת "שם פריט".')
        else:
            all_classes = sorted(df_cust[COL_CLASS].dropna().astype(str).unique().tolist()) if COL_CLASS in df_cust.columns else []

            f1, f2 = st.columns([1, 2], gap="small")
            with f1:
                cls_pick = st.selectbox(
                    "סינון לפי קוד מיון",
                    options=["(הכל)"] + all_classes,
                    index=0,
                    key=f"items_cls_pick::{context_username}::{context_agent_id}::{account}",
                )
            with f2:
                item_search = st.text_input(
                    "חיפוש בשם פריט (מכיל)",
                    value="",
                    key=f"items_search::{context_username}::{context_agent_id}::{account}",
                )

            items_df = df_cust.copy()
            if cls_pick and cls_pick != "(הכל)" and COL_CLASS in items_df.columns:
                items_df = items_df[items_df[COL_CLASS].astype(str) == str(cls_pick)].copy()

            if item_search.strip():
                s = item_search.strip().lower()
                items_df[COL_ITEM] = items_df[COL_ITEM].astype(str)
                items_df = items_df[items_df[COL_ITEM].str.lower().str.contains(s, na=False)].copy()

            if items_df.empty:
                st.caption("אין פריטים שמתאימים לסינון הנוכחי.")
            else:
                grp_cols = [COL_CLASS, COL_ITEM] if COL_CLASS in items_df.columns else [COL_ITEM]

                agg_map = {}
                if CAN_SEE_MONEY:
                    agg_map["מכירות_בכסף"] = (COL_NET, "sum")
                if CAN_SEE_QTY:
                    agg_map["מכירות_בכמות"] = (COL_QTY, "sum")

                if not agg_map:
                    st.caption("אין עמודות סכימה לתצוגה לפי ההרשאות הנוכחיות.")
                else:
                    g = items_df.groupby(grp_cols, dropna=False).agg(**agg_map).reset_index()

                    if CAN_SEE_MONEY and CAN_SEE_QTY and "מכירות_בכסף" in g.columns and "מכירות_בכמות" in g.columns:
                        g["מחיר_ממוצע"] = g.apply(
                            lambda r: safe_div(float(r["מכירות_בכסף"]), float(r["מכירות_בכמות"])), axis=1
                        )
                    else:
                        g["מחיר_ממוצע"] = math.nan

                    def item_delta_row(r) -> float:
                        cls_v = str(r[COL_CLASS]) if COL_CLASS in r else ""
                        item_v = str(r[COL_ITEM])
                        return float(user_item_qty.get((str(context_username), str(account), cls_v, item_v), 0.0) or 0.0)

                    g["תוספת_יעד_כמות"] = g.apply(item_delta_row, axis=1)

                    if "מכירות_בכמות" in g.columns:
                        g["יעד_בכמות"] = (
                            pd.to_numeric(g["מכירות_בכמות"], errors="coerce").fillna(0.0)
                            + pd.to_numeric(g["תוספת_יעד_כמות"], errors="coerce").fillna(0.0)
                        )
                    else:
                        g["יעד_בכמות"] = pd.to_numeric(g["תוספת_יעד_כמות"], errors="coerce").fillna(0.0)

                    if CAN_SEE_MONEY and "מכירות_בכסף" in g.columns:
                        dm = (
                            pd.to_numeric(g["תוספת_יעד_כמות"], errors="coerce").fillna(0.0)
                            * pd.to_numeric(g["מחיר_ממוצע"], errors="coerce").fillna(0.0)
                        )
                        g["תוספת_יעד_כסף"] = dm
                        g["יעד_בכסף"] = pd.to_numeric(g["מכירות_בכסף"], errors="coerce").fillna(0.0) + dm

                    # monthly
                    if "מכירות_בכמות" in g.columns:
                        g["ממוצע_חודשי_כמות_2025"] = pd.to_numeric(g["מכירות_בכמות"], errors="coerce").fillna(0.0) / MONTHS_IN_YEAR
                    else:
                        g["ממוצע_חודשי_כמות_2025"] = 0.0
                    g["תוספת_חודשית_כמות"] = pd.to_numeric(g["תוספת_יעד_כמות"], errors="coerce").fillna(0.0) / MONTHS_IN_YEAR
                    g["יעד_חודשי_כמות_2026"] = pd.to_numeric(g["יעד_בכמות"], errors="coerce").fillna(0.0) / MONTHS_IN_YEAR

                    disp = g.copy()
                    if COL_CLASS in disp.columns:
                        disp = disp.rename(columns={COL_CLASS: "קוד מיון"})
                    disp = disp.rename(columns={COL_ITEM: "שם פריט"})

                    sort_col = "מכירות_בכסף" if ("מכירות_בכסף" in disp.columns) else (
                        "מכירות_בכמות" if "מכירות_בכמות" in disp.columns else "תוספת_יעד_כמות"
                    )
                    disp = disp.sort_values(sort_col, ascending=False).reset_index(drop=True)

                    editor_cols = []
                    if "קוד מיון" in disp.columns:
                        editor_cols += ["קוד מיון"]
                    editor_cols += ["שם פריט"]

                    if "מכירות_בכמות" in disp.columns:
                        editor_cols += ["מכירות_בכמות", "ממוצע_חודשי_כמות_2025"]

                    editor_cols += ["תוספת_יעד_כמות", "תוספת_חודשית_כמות", "יעד_בכמות", "יעד_חודשי_כמות_2026"]

                    if CAN_SEE_MONEY and "מכירות_בכסף" in disp.columns:
                        editor_cols += ["מכירות_בכסף", "מחיר_ממוצע", "תוספת_יעד_כסף", "יעד_בכסף"]

                    editor_cols = [c for c in editor_cols if c in disp.columns]

                    item_cfg = {
                        "שם פריט": st.column_config.TextColumn("שם פריט", disabled=True),
                        "תוספת_יעד_כמות": st.column_config.NumberColumn("תוספת יעד (כמות) — לפי פריט", step=1.0, format="%.2f"),
                        "תוספת_חודשית_כמות": st.column_config.NumberColumn("תוספת חודשית (כמות)", disabled=True, format="%.2f"),
                        "יעד_בכמות": st.column_config.NumberColumn("כמות 2026", disabled=True, format="%.2f"),
                        "יעד_חודשי_כמות_2026": st.column_config.NumberColumn("יעד חודשי 2026 (כמות)", disabled=True, format="%.2f"),
                    }
                    if "קוד מיון" in disp.columns:
                        item_cfg["קוד מיון"] = st.column_config.TextColumn("קוד מיון", disabled=True)
                    if "מכירות_בכמות" in disp.columns:
                        item_cfg["מכירות_בכמות"] = st.column_config.NumberColumn("כמות 2025", disabled=True, format="%.2f")
                    if "ממוצע_חודשי_כמות_2025" in disp.columns:
                        item_cfg["ממוצע_חודשי_כמות_2025"] = st.column_config.NumberColumn("ממוצע חודשי (כמות) 2025", disabled=True, format="%.2f")

                    if CAN_SEE_MONEY:
                        if "מכירות_בכסף" in disp.columns:
                            item_cfg["מכירות_בכסף"] = st.column_config.NumberColumn("מכירות (₪)", disabled=True, format="%.2f")
                        if "מחיר_ממוצע" in disp.columns:
                            item_cfg["מחיר_ממוצע"] = st.column_config.NumberColumn("מחיר ממוצע", disabled=True, format="%.2f")
                        if "תוספת_יעד_כסף" in disp.columns:
                            item_cfg["תוספת_יעד_כסף"] = st.column_config.NumberColumn("תוספת יעד (₪)", disabled=True, format="%.2f")
                        if "יעד_בכסף" in disp.columns:
                            item_cfg["יעד_בכסף"] = st.column_config.NumberColumn("2026 (₪)", disabled=True, format="%.2f")

                    with st.form(key=f"items_form::{context_username}::{context_agent_id}::{account}", clear_on_submit=False):
                        edited_items = st.data_editor(
                            disp[editor_cols],
                            hide_index=True,
                            use_container_width=True,
                            column_config=item_cfg,
                            key=f"items_editor::{context_username}::{context_agent_id}::{account}::{cls_pick}::{item_search}",
                        )
                        c1, c2 = st.columns([1, 1], gap="small")
                        with c1:
                            items_refresh = st.form_submit_button("רענן חישוב פריטים", use_container_width=True)
                        with c2:
                            items_save = st.form_submit_button("שמור פריטים למסד", use_container_width=True)

                    if items_refresh or items_save:
                        edited_items["תוספת_יעד_כמות"] = pd.to_numeric(edited_items["תוספת_יעד_כמות"], errors="coerce").fillna(0.0)

                        for _, rr in edited_items.iterrows():
                            cls_val = str(rr.get("קוד מיון", "")) if "קוד מיון" in edited_items.columns else ""
                            item_val = str(rr["שם פריט"])
                            dq = float(rr["תוספת_יעד_כמות"] or 0.0)

                            user_item_qty[(str(context_username), str(account), str(cls_val), str(item_val))] = dq

                            if items_save:
                                sales_year_qty = float(rr.get("מכירות_בכמות", 0.0) or 0.0)
                                monthly_avg_2025_qty = sales_year_qty / MONTHS_IN_YEAR
                                monthly_add_qty = dq / MONTHS_IN_YEAR
                                db_upsert_user_item_qty(
                                    con,
                                    context_username,
                                    account,
                                    cls_val,
                                    item_val,
                                    dq,
                                    monthly_avg_2025_qty,
                                    monthly_add_qty,
                                )

                        st.session_state[item_key] = user_item_qty
                        st.success("נשמרו פריטים ועודכן." if items_save else "עודכן (ללא שמירה).")
                        # FIX: force rebuild to show computed columns
                        st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)
