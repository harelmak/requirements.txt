# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v8.9.3 (FINAL ADMIN LOCK)
"""

import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path

# =========================
# הגדרות קבועות
# =========================
ADMIN_USERNAME = "ADMIN"
ADMIN_PASSWORD = "1511!!"

st.set_page_config(page_title="Uzeb — Targets", layout="wide")

# עיצוב RTL בסיסי
st.markdown("<style>html, body, [class*='css'] { direction: rtl; text-align: right; }</style>", unsafe_allow_html=True)

# =========================
# פונקציות בסיס
# =========================
def check_auth():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.title("התחברות למערכת Uzeb")
        user = st.text_input("שם משתמש")
        pwd = st.text_input("סיסמה", type="password")
        if st.button("התחבר"):
            if user == ADMIN_USERNAME and pwd == ADMIN_PASSWORD:
                st.session_state.authenticated = True
                st.session_state.username = ADMIN_USERNAME
                st.rerun()
            elif user != "" and pwd != "":
                st.session_state.authenticated = True
                st.session_state.username = user
                st.rerun()
            else:
                st.error("פרטים שגויים")
        return False
    return True

def main():
    if not check_auth():
        return

    st.sidebar.write(f"מחובר כ: **{st.session_state.username}**")
    if st.sidebar.button("התנתק"):
        st.session_state.authenticated = False
        st.rerun()

    # ==========================================
    # ניהול טאבים לפי הרשאות - כאן השינוי המרכזי!
    # ==========================================
    
    # 1. הגדרת רשימת הטאבים הזמינים
    tab_list = ["דאשבורד", "צפייה בנתונים"]
    
    # רק אם המשתמש הוא ADMIN, נוסיף את הטאב של עריכת יעדים
    is_admin = (st.session_state.username == ADMIN_USERNAME)
    if is_admin:
        tab_list.append("עריכת יעדים (לקוח יחיד)")

    tabs = st.tabs(tab_list)

    # --- טאב 1: דאשבורד (לכולם) ---
    with tabs[0]:
        st.header("לוח בקרה")
        st.write("נתונים כלליים...")

    # --- טאב 2: צפייה בנתונים (לכולם) ---
    with tabs[1]:
        st.header("נתוני מכירות")
        st.write("כאן כולם רואים נתונים ב-View Only.")

    # --- טאב 3: עריכת יעדים (ADMIN בלבד) ---
    if is_admin:
        with tabs[2]:
            st.header("🔧 עריכת יעדים (לקוח יחיד)")
            st.info("ממשק זה זמין עבורך בלבד כמנהל.")
            
            # כאן תבוא הטבלה שרק המנהל יכול לראות ולערוך
            # לדוגמה:
            # df_targets = load_data_from_sqlite()
            # edited_df = st.data_editor(df_targets)
            # save_to_sqlite(edited_df)
            
            st.write("טבלת עריכה מוצגת כאן...")

if __name__ == "__main__":
    main()
