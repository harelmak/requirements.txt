# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v9.3.0 (BACK TO CLASSIC + FIX)
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
DB_FILE = "uzeb_data.db"

st.set_page_config(page_title="Uzeb — Targets", layout="wide")

# עיצוב RTL בסיסי
st.markdown("<style>html, body, [class*='css'] { direction: rtl; text-align: right; }</style>", unsafe_allow_html=True)

# =========================
# פונקציות בסיס
# =========================
def get_connection():
    return sqlite3.connect(DB_FILE)

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

    # טעינת נתונים
    try:
        with get_connection() as conn:
            df = pd.read_sql("SELECT * FROM sales_targets", conn)
    except:
        df = pd.DataFrame()

    # --- ניהול טאבים לפי הרשאות (המבנה המקורי) ---
    tab_list = ["דאשבורד", "צפייה בנתונים"]
    is_admin = (st.session_state.username == ADMIN_USERNAME)
    if is_admin:
        tab_list.append("עריכת יעדים (לקוח יחיד)")

    tabs = st.tabs(tab_list)

    # --- טאב 1: דאשבורד ---
    with tabs[0]:
        st.header("לוח בקרה")
        if not df.empty:
            st.write(f"סה''כ שורות במערכת: {len(df)}")
        else:
            st.write("אין נתונים להצגה.")

    # --- טאב 2: צפייה בנתונים (כאן ביצעתי את תיקון הסינון) ---
    with tabs[1]:
        st.header("נתוני מכירות")
        if not df.empty:
            # סינון לפי קבוצת מיון
            col_name = "קבוצת מיון" if "קבוצת מיון" in df.columns else df.columns[0]
            categories = sorted(df[col_name].unique().tolist())
            
            selected_cat = st.selectbox("סנן לפי קבוצת מיון:", ["הצג הכל"] + categories)
            
            if selected_cat != "הצג הכל":
                display_df = df[df[col_name] == selected_cat]
            else:
                display_df = df
                
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("כאן כולם רואים נתונים ב-View Only. כרגע אין נתונים.")

    # --- טאב 3: עריכת יעדים (ADMIN בלבד - טעינה מחדש נקייה) ---
    if is_admin:
        with tabs[2]:
            st.header("🔧 ניהול נתונים")
            
            # אפשרות העלאת קובץ חדש (החלפה נקייה)
            uploaded_file = st.file_uploader("העלה קובץ אקסל חדש (xlsx)", type="xlsx")
            if st.button("עדכן נתונים ודרוס קודמים"):
                if uploaded_file:
                    new_df = pd.read_excel(uploaded_file)
                    with get_connection() as conn:
                        # שימוש ב-replace מבטיח שהנתונים הישנים נמחקים
                        new_df.to_sql("sales_targets", conn, if_exists="replace", index=False)
                    st.success("הנתונים עודכנו בהצלחה!")
                    st.rerun()
            
            st.write("---")
            st.write("טבלת עריכה ידנית:")
            if not df.empty:
                edited_df = st.data_editor(df)
                if st.button("שמור שינויים בטבלה"):
                    with get_connection() as conn:
                        edited_df.to_sql("sales_targets", conn, if_exists="replace", index=False)
                    st.success("השינויים נשמרו!")

if __name__ == "__main__":
    main()
