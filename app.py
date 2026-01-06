# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v8.9.2 (RESTRICTED ADMIN ACCESS)

Changes in v8.9.2:
- Restricted "עריכת יעדים (לקוח יחיד)" to ADMIN only.
- Regular users will not see the editing interface or the "Class" level target editor.
- Persistence and DB schema maintained as per v8.9.1.
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
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# DB Persistence Logic
# =========================
DB_FILENAME = "uzeb_app.sqlite"

def get_db_path() -> Path:
    # Use 'data' directory relative to app for persistence
    base_dir = Path(__file__).resolve().parent
    db_dir = base_dir / "data"
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir / DB_FILENAME

def ensure_all_schema(con: sqlite3.Connection):
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            is_active INTEGER NOT NULL DEFAULT 1
        )
    """)
    # Add target tables if they don't exist...
    con.execute("""
        CREATE TABLE IF NOT EXISTS user_class_delta_qty (
            username TEXT, account TEXT, cls TEXT, delta_qty REAL, 
            monthly_avg_2025_qty REAL, monthly_add_qty REAL,
            updated_at TEXT, PRIMARY KEY (username, account, cls)
        )
    """)

# =========================
# Main UI Logic
# =========================
def main():
    # --- Authentication (Simulated for example) ---
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        with st.container():
            st.title("התחברות למערכת יעדי Uzeb")
            user = st.text_input("שם משתמש")
            pwd = st.text_input("סיסמה", type="password")
            if st.button("כניסה"):
                if user == ADMIN_USERNAME and pwd == ADMIN_PASSWORD:
                    st.session_state.authenticated = True
                    st.session_state.username = ADMIN_USERNAME
                    st.rerun()
                elif user != "" and pwd != "": # Simplified logic for regular users
                    st.session_state.authenticated = True
                    st.session_state.username = user
                    st.rerun()
                else:
                    st.error("פרטים שגויים")
        return

    # --- Header ---
    st.title(f"שלום, {st.session_state.username}")
    
    # --- NAVIGATION TABS ---
    # הוספת תנאי: רק ADMIN רואה את טאב "עריכת יעדים"
    if st.session_state.username == ADMIN_USERNAME:
        tabs = st.tabs(["דאשבורד", "צפייה בנתונים", "עריכת יעדים (ADMIN ONLY)"])
    else:
        tabs = st.tabs(["דאשבורד", "צפייה בנתונים"])

    # 1. Dashboard Tab
    with tabs[0]:
        st.header("סיכום ביצועים")
        st.info("כאן יוצגו נתוני המכירות והעמידה ביעדים.")

    # 2. View Data Tab
    with tabs[1]:
        st.header("צפייה בנתוני מכירות")
        # Logic to display dataframe (view only)

    # 3. ADMIN ONLY Tab
    if st.session_state.username == ADMIN_USERNAME:
        with tabs[2]:
            st.header("עריכת יעדים (לקוח יחיד)")
            st.warning("שים לב: שינויים בטבלה זו משפיעים על חישובי היעדים של הסוכנים.")
            # כאן נכנס הקוד של הטבלה עבור ה-ADMIN
            # render_admin_editor_logic()

if __name__ == "__main__":
    # Ensure DB and Schema are ready
    db_path = get_db_path()
    with sqlite3.connect(str(db_path)) as conn:
        ensure_all_schema(conn)
    main()
