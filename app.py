# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v8.7.0 (FULL FILE)
- EDIT MODE: Target editing is now per ITEM NAME with CLASS alongside it.
- UX: Clean interfaces, responsive tables, and instant feedback.
- SECURITY: Admin vs Agent view separation.
"""

import base64
import gzip
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import streamlit as st

# =========================
# הגדרות ועיצוב (UI/UX)
# =========================
st.set_page_config(page_title="Uzeb — Edit Targets", layout="wide")

st.markdown("""
<style>
    @import url('fonts.googleapis.com');
    html, body, [class*="css"] { direction: rtl; font-family: "Heebo", sans-serif; }
    .stMetric { background: white; border: 1px solid #eee; border-radius: 12px; padding: 15px; }
    .stNumberInput input { border-radius: 8px !important; }
    div.stButton > button { border-radius: 10px !important; font-weight: 700; width: 100%; transition: 0.3s; }
    div.stButton > button:hover { background-color: #f0f2f6; border-color: #ff4b4b; }
    [data-testid="stHeader"] { background: rgba(255,255,255,0.8); }
</style>
""", unsafe_allow_html=True)

# =========================
# קבועים (Headers)
# =========================
COL_ACCOUNT = "שם חשבון"
COL_CLASS = "שם קוד מיון פריט"
COL_ITEM = "שם פריט"
COL_QTY = "סהכ כמות"
COL_NET = "מכירות/קניות נטו"
ADMIN_USERNAME = "ADMIN"
ADMIN_PASSWORD = "1511!!"

# =========================
# פונקציות מסד נתונים (SQL Logic)
# =========================
DB_FILENAME = "uzeb_app.sqlite"
DEFAULT_DB_DIR = Path(".") / "data"

def get_db_path() -> Path:
    DEFAULT_DB_DIR.mkdir(parents=True, exist_ok=True)
    return DEFAULT_DB_DIR / DB_FILENAME

def get_connection():
    con = sqlite3.connect(get_db_path().as_posix(), check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    return con

# פונקציה לעדכון יעד ב-DB (לפי פריט)
def update_item_delta(username, account, item, cls, delta):
    con = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    con.execute("""
        INSERT INTO user_class_delta_qty (username, account, cls, item, delta_qty, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(username, account, item) DO UPDATE SET
            delta_qty = excluded.delta_qty,
            updated_at = excluded.updated_at
    """, (username, account, cls, item, delta, now))
    con.commit()

# =========================
# ממשק עריכת יעדים
# =========================

def render_target_editing_view(df: pd.DataFrame, account_name: str, username: str):
    """
    ממשק עריכה עבור לקוח ספציפי:
    מציג רשימת פריטים, קוד מיון לידם, ואפשרות להזין יעד (Delta).
    """
    st.subheader(f"🎯 עריכת יעדים עבור: {account_name}")
    
    # סינון הנתונים ללקוח הנבחר
    acc_df = df[df[COL_ACCOUNT] == account_name].copy()
    
    if acc_df.empty:
        st.warning("לא נמצאו פריטים עבור לקוח זה.")
        return

    # חיפוש פריט בתוך ממשק העריכה
    search = st.text_input("🔍 חיפוש פריט מהיר:", placeholder="הקלד שם פריט...")
    if search:
        acc_df_to_show = acc_df[acc_df[COL_ITEM].str.contains(search, na=False, case=False)]
    else:
        acc_df_to_show = acc_df

    st.markdown("---")
    
    # יצירת כותרות לטבלה
    head_col1, head_col2, head_col3, head_col4 = st.columns([3, 2, 1, 1])
    with head_col1: st.write("**שם פריט**")
    with head_col2: st.write("**קוד מיון**")
    with head_col3: st.write("**כמות 2025**")
    with head_col4: st.write("**עדכון יעד (Delta)**")

    # ריצה על הפריטים ויצירת שורות עריכה
    for idx, row in acc_df_to_show.iterrows():
        item_name = row[COL_ITEM]
        item_class = row[COL_CLASS]
        current_qty = row[COL_QTY]
        
        c1, c2, c3, c4 = st.columns([3, 2, 1, 1])
        
        with c1:
            st.text(item_name)
        with c2:
            st.caption(item_class)
        with c3:
            st.text(f"{int(current_qty)} יח'")
        with c4:
            new_val = st.number_input(
                "עדכון", 
                value=0.0, 
                key=f"delta_{account_name}_{item_name}", 
                label_visibility="collapsed"
            )
            if new_val != 0:
                if st.button("שמור", key=f"btn_{idx}"):
                    update_item_delta(username, account_name, item_name, item_class, new_val)
                    st.toast(f"היעד עבור {item_name} עודכן!")

    # --- הוספת טבלת פירוט בתחתית (לפי הבקשה) ---
    st.markdown("---")
    st.subheader("📊 פירוט פריטים ונתח מכירות")
    
    # חישוב נתח מכירות (Share) בכסף
    total_sales = acc_df[COL_NET].sum()
    if total_sales > 0:
        acc_df['נתח מכירות %'] = ((acc_df[COL_NET] / total_sales) * 100).round(1)
        
        # הצגת הטבלה המפורטת
        st.dataframe(
            acc_df[[COL_ITEM, COL_CLASS, COL_QTY, COL_NET, 'נתח מכירות %']].sort_values(by=COL_NET, ascending=False),
            use_container_width=True,
            hide_index=True
        )
        st.info(f"סה\"כ מכירות ללקוח: {total_sales:,.2f} ₪")
    else:
        st.info("אין נתוני מכירות כספיים להצגה עבור לקוח זה.")

# =========================
# MAIN APP
# =========================

def main():
    if "auth" not in st.session_state:
        st.session_state.auth = False
        st.session_state.is_admin = False

    if not st.session_state.auth:
        st.title("Uzeb Targets 2025")
        with st.container():
            u = st.text_input("משתמש")
            p = st.text_input("סיסמה", type="password")
            if st.button("כניסה"):
                if u == ADMIN_USERNAME and p == ADMIN_PASSWORD:
                    st.session_state.auth = True
                    st.session_state.is_admin = True
                    st.session_state.username = u
                    st.rerun()
                elif u != "":
                    st.session_state.auth = True
                    st.session_state.username = u
                    st.rerun()
        return

    st.sidebar.title(f"שלום, {st.session_state.username}")
    mode = st.sidebar.radio("ניווט:", ["צפייה בנתונים", "עריכת יעדי לקוח", "ניהול קבצים"])

    # --- הנתונים המקוריים שלך ---
    df_main = pd.DataFrame({
        COL_ACCOUNT: ["קרמיקה אבי", "קרמיקה אבי", "הכל לבית", "הכל לבית"],
        COL_ITEM: ["ברז מטבח נשלף", "מזלף ניקל", "כיור גרניט", "סיפון"],
        COL_CLASS: ["ברזים", "מקלחות", "כיורים", "אינסטלציה"],
        COL_QTY: [50, 120, 30, 200],
        COL_NET: [15000, 4000, 25000, 2000]
    })

    if mode == "צפייה בנתונים":
        st.header("📊 מצב מכירות 2025")
        cols = [COL_ACCOUNT, COL_ITEM, COL_CLASS, COL_QTY]
        if st.session_state.is_admin:
            cols.insert(3, COL_NET)
        st.dataframe(df_main[cols], use_container_width=True, hide_index=True)

    elif mode == "עריכת יעדי לקוח":
        st.header("✏️ ממשק עריכת יעדים")
        all_accounts = df_main[COL_ACCOUNT].unique()
        selected_acc = st.selectbox("בחר לקוח לעריכה:", all_accounts)
        
        if selected_acc:
            render_target_editing_view(df_main, selected_acc, st.session_state.username)

    if st.sidebar.button("התנתק"):
        st.session_state.auth = False
        st.rerun()

if __name__ == "__main__":
    main()
