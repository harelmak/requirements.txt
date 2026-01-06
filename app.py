# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v8.9.4 (FIXED DATABASE SYNC)
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

# עיצוב RTL
st.markdown("<style>html, body, [class*='css'] { direction: rtl; text-align: right; }</style>", unsafe_allow_html=True)

# =========================
# פונקציות בסיס נתונים (סעיף 2 - מחיקה ועדכון)
# =========================

def get_connection():
    return sqlite3.connect(DB_FILE)

def upload_and_refresh_data(uploaded_file):
    """
    פונקציה זו קוראת את האקסל ודורסת את הנתונים הישנים ב-SQL
    כך שקודים שגויים יימחקו לצמיתות.
    """
    try:
        # קריאת האקסל
        df = pd.read_excel(uploaded_file)
        
        # ניקוי בסיסי של רווחים מיותרים בטקסט (שמונע טעויות מיון)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        with get_connection() as conn:
            # שימוש ב-replace מוחק את הטבלה הישנה ויוצר חדשה
            # זה פותר את בעיית הנתונים ה"תקועים"
            df.to_sql("sales_targets", conn, if_exists="replace", index=False)
            
        return True, "הנתונים עודכנו בהצלחה! בסיס הנתונים נוקה ורוענן."
    except Exception as e:
        return False, f"שגיאה בעדכון הנתונים: {e}"

def load_data():
    try:
        with get_connection() as conn:
            return pd.read_sql("SELECT * FROM sales_targets", conn)
    except:
        return pd.DataFrame() # מחזיר טבלה ריקה אם אין עדיין נתונים

# =========================
# ניהול הרשאות
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

# =========================
# ממשק ראשי
# =========================
def main():
    if not check_auth():
        return

    st.sidebar.write(f"מחובר כ: **{st.session_state.username}**")
    if st.sidebar.button("התנתק"):
        st.session_state.authenticated = False
        st.rerun()

    is_admin = (st.session_state.username == ADMIN_USERNAME)
    
    # הגדרת הטאבים
    tab_titles = ["📊 דאשבורד", "🔍 צפייה בנתונים"]
    if is_admin:
        tab_titles.append("⚙️ ניהול וטעינת נתונים")

    tabs = st.tabs(tab_titles)

    # --- טאב 1: דאשבורד ---
    with tabs[0]:
        st.header("לוח בקרה")
        df = load_data()
        if df.empty:
            st.warning("אין נתונים להצגה. יש לטעון קובץ אקסל בטאב ניהול.")
        else:
            st.metric("סה''כ שורות במערכת", len(df))
            st.write("סיכום נתונים כללי מוצג כאן.")

    # --- טאב 2: צפייה בנתונים ---
    with tabs[1]:
        st.header("נתוני מכירות (View Only)")
        df = load_data()
        if not df.empty:
            # הוספת חיפוש/סינון מהיר
            search_term = st.text_input("חיפוש חופשי (קוד מיון, שם פריט וכו'):")
            if search_term:
                mask = df.astype(str).apply(lambda x: x.str.contains(search_term)).any(axis=1)
                df = df[mask]
            st.dataframe(df, use_container_width=True)
        else:
            st.info("בסיס הנתונים ריק.")

    # --- טאב 3: ניהול נתונים (ADMIN בלבד) ---
    if is_admin:
        with tabs[2]:
            st.header("🔧 ניהול בסיס נתונים")
            
            st.subheader("1. העלאת נתונים חדשים")
            st.info("שימוש באפשרות זו ימחק את כל הנתונים הקיימים בבסיס הנתונים ויחליפם בנתונים מהאקסל החדש.")
            
            uploaded_file = st.file_uploader("בחר קובץ אקסל מעודכן (xlsx)", type=["xlsx"])
            
            if st.button("בצע עדכון וניקוי בסיס נתונים"):
                if uploaded_file:
                    success, msg = upload_and_refresh_data(uploaded_file)
                    if success:
                        st.success(msg)
                        st.balloons()
                    else:
                        st.error(msg)
                else:
                    st.warning("נא לבחור קובץ תחילה.")

            st.divider()
            
            st.subheader("2. עריכה ידנית")
            st.write("ממשק עריכה ישיר לטבלה:")
            df_to_edit = load_data()
            if not df_to_edit.empty:
                edited_df = st.data_editor(df_to_edit, key="admin_editor")
                if st.button("שמור שינויים ידניים"):
                    with get_connection() as conn:
                        edited_df.to_sql("sales_targets", conn, if_exists="replace", index=False)
                    st.success("השינויים נשמרו!")
            else:
                st.write("אין נתונים לעריכה.")

if __name__ == "__main__":
    main()
