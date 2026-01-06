# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v8.9.8 (ULTIMATE DATABASE PURGE)
"""

import sqlite3
import pandas as pd
import streamlit as st
import os
import gc  # Garbage Collector לניקוי זיכרון

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
# פונקציות ניקוי וסנכרון (סעיף 2 המורחב)
# =========================

def hard_reset_and_upload(uploaded_file):
    """
    מבצע מחיקה פיזית של הקובץ וניקוי זיכרון לפני טעינה חדשה
    """
    try:
        # 1. קריאת הקובץ החדש לזיכרון לפני שנוגעים ב-DB
        df_new = pd.read_excel(uploaded_file)
        df_new = df_new.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df_new = df_new.drop_duplicates()

        # 2. ניקוי ה-Cache של Streamlit (חשוב מאוד!)
        st.cache_data.clear()
        
        # 3. סגירת כל החיבורים ומחיקת קובץ ה-DB הקיים מהדיסק
        if os.path.exists(DB_FILE):
            # ניסיון למחוק את הקובץ פיזית כדי להבטיח שאין זכר לנתונים ישנים
            try:
                os.remove(DB_FILE)
            except:
                # אם הקובץ נעול, נרוקן את הטבלה ידנית בשיטה אגרסיבית
                with sqlite3.connect(DB_FILE) as conn:
                    conn.execute("DROP TABLE IF EXISTS sales_targets")
                    conn.execute("VACUUM") # דחיסת הקובץ ומחיקת תוכן פיזי
                
        # 4. יצירת בסיס נתונים חדש לגמרי מהאקסל הנקי
        with sqlite3.connect(DB_FILE) as conn:
            df_new.to_sql("sales_targets", conn, if_exists="replace", index=False)
        
        return True, f"בוצע איפוס קשיח! נטענו {len(df_new)} שורות מהאקסל בלבד."
    except Exception as e:
        return False, f"שגיאה קריטית: {e}"

def load_data():
    @st.cache_data
    def fetch():
        if not os.path.exists(DB_FILE):
            return pd.DataFrame()
        try:
            with sqlite3.connect(DB_FILE) as conn:
                return pd.read_sql("SELECT * FROM sales_targets", conn)
        except:
            return pd.DataFrame()
    return fetch()

# =========================
# ממשק המערכת
# =========================
def main():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.title("התחברות למערכת Uzeb")
        u = st.text_input("שם משתמש")
        p = st.text_input("סיסמה", type="password")
        if st.button("כניסה"):
            if u == ADMIN_USERNAME and p == ADMIN_PASSWORD:
                st.session_state.authenticated = True
                st.rerun()
        return

    # תפריט עליון
    tabs = st.tabs(["📊 דאשבורד", "🔍 רשימת מוצרים", "🛑 ניהול ואיפוס (ADMIN)"])

    with tabs[0]:
        st.header("סיכום נתונים")
        df = load_data()
        if not df.empty:
            st.success(f"כרגע מוצגות {len(df)} שורות בבסיס הנתונים.")
        else:
            st.warning("אין נתונים במערכת.")

    with tabs[1]:
        st.header("תצוגת נתונים מה-DB")
        df = load_data()
        if not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.info("בסיס הנתונים ריק.")

    with tabs[2]:
        st.header("מנגנון איפוס בסיס נתונים")
        st.error("שים לב: פעולה זו תמחוק את קובץ ה-DB הקיים ותבנה אותו מחדש רק מהאקסל שתעלה.")
        
        file = st.file_uploader("העלה אקסל (XLSX) - וודא שזה הקובץ הנקי", type=["xlsx"])
        
        if st.button("🔥 בצע איפוס קשיח וטעינה מחדש"):
            if file:
                success, msg = hard_reset_and_upload(file)
                if success:
                    st.success(msg)
                    st.balloons()
                    # השהיה קלה וריענון
                    st.rerun()
                else:
                    st.error(msg)
            else:
                st.warning("נא לבחור קובץ אקסל.")

if __name__ == "__main__":
    main()
