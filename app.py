# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v9.2.0 (CLEAN SYNC & DYNAMIC FILTER)
"""

import sqlite3
import pandas as pd
import streamlit as st
import os

# =========================
# הגדרות קבועות
# =========================
ADMIN_USERNAME = "ADMIN"
ADMIN_PASSWORD = "1511!!"
DB_FILE = "uzeb_data.db"

st.set_page_config(page_title="Uzeb — Targets", layout="wide")

# עיצוב RTL (תמיכה בעברית)
st.markdown("""
<style>
    html, body, [class*='css'] { direction: rtl; text-align: right; }
    .stSelectbox label { font-size: 20px !important; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# =========================
# פונקציות ליבה (שלב 2 - ניקוי ואיפוס)
# =========================

def get_connection():
    return sqlite3.connect(DB_FILE)

def refresh_database_from_excel(uploaded_file):
    """
    מבצע מחיקה מוחלטת של הנתונים הישנים וטעינה נקייה בלבד.
    """
    try:
        # קריאת הגיליון הראשון מהאקסל
        df = pd.read_excel(uploaded_file, sheet_name=0)
        
        # ניקוי בסיסי: הסרת שורות ריקות ורווחים מיותרים בשמות הקטגוריות
        df = df.dropna(how='all')
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        with get_connection() as conn:
            # שלב 2: מחיקת הטבלה הקיימת ויצירתה מחדש (DROP)
            # זה מבטיח שנתונים שלא קיימים באקסל לא יופיעו ב-DB
            conn.execute("DROP TABLE IF EXISTS sales_targets")
            df.to_sql("sales_targets", conn, if_exists="replace", index=False)
            conn.execute("VACUUM") # ניקוי פיזי של הדיסק
            
        st.cache_data.clear() # ניקוי ה-Cache של השרת
        return True, f"הנתונים רועננו! נטענו {len(df)} שורות מהקובץ החדש."
    except Exception as e:
        return False, f"שגיאה בעדכון: {e}"

def load_data():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame()
    with get_connection() as conn:
        try:
            return pd.read_sql("SELECT * FROM sales_targets", conn)
        except:
            return pd.DataFrame()

# =========================
# ניהול הרשאות
# =========================
def check_auth():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.title("מערכת ניהול יעדים Uzeb")
        user = st.text_input("שם משתמש")
        pwd = st.text_input("סיסמה", type="password")
        if st.button("התחבר"):
            if (user == ADMIN_USERNAME and pwd == ADMIN_PASSWORD) or (user != "" and pwd != ""):
                st.session_state.authenticated = True
                st.session_state.username = user
                st.rerun()
            else:
                st.error("פרטים שגויים")
        return False
    return True

# =========================
# ממשק המערכת
# =========================
def main():
    if not check_auth():
        return

    st.sidebar.write(f"מחובר כ: **{st.session_state.username}**")
    if st.sidebar.button("התנתק"):
        st.session_state.authenticated = False
        st.rerun()

    is_admin = (st.session_state.username == ADMIN_USERNAME)
    
    # טאבים לפי הרשאות
    tab_list = ["📊 תצוגת יעדים", "📑 צפייה בנתונים"]
    if is_admin:
        tab_list.append("⚙️ ניהול אדמין (איפוס וטעינה)")

    tabs = st.tabs(tab_list)

    # --- טאב 1: תצוגת יעדים עם סינון דינמי ---
    with tabs[0]:
        st.header("🔍 סינון לפי קבוצת מיון")
        df = load_data()
        
        if df.empty:
            st.warning("אין נתונים בבסיס הנתונים. מנהל צריך לטעון קובץ אקסל.")
        else:
            # זיהוי עמודת הסינון (מניחים שקוראים לה 'קבוצת מיון')
            filter_col = "קבוצת מיון" if "קבוצת מיון" in df.columns else df.columns[0]
            
            # רשימת קטגוריות ייחודיות
            options = sorted(df[filter_col].unique().tolist())
            
            # תיבת הבחירה - הסינון הדינמי
            selected = st.selectbox("בחר קבוצה להצגה:", ["הצג הכל"] + options)

            # פילטור הטבלה
            if selected != "הצג הכל":
                filtered_df = df[df[filter_col] == selected]
            else:
                filtered_df = df

            st.write(f"מציג **{len(filtered_df)}** שורות:")
            st.dataframe(filtered_df, use_container_width=True, height=500)

    # --- טאב 2: צפייה בנתונים (View Only) ---
    with tabs[1]:
        st.header("נתוני מכירות מלאים")
        full_df = load_data()
        if not full_df.empty:
            st.dataframe(full_df, use_container_width=True)
        else:
            st.info("בסיס הנתונים ריק.")

    # --- טאב 3: ניהול אדמין (שלב 2) ---
    if is_admin:
        with tabs[2]:
            st.header("⚙️ ממשק ניהול - איפוס וסנכרון")
            st.info("כאן ניתן לנקות את המערכת מנתונים ישנים ולהעלות אקסל חדש.")
            
            uploaded_file = st.file_uploader("בחר קובץ אקסל מעודכן (XLSX)", type=["xlsx"])
            
            if st.button("🔥 בצע איפוס קשיח וטעינה מחדש"):
                if uploaded_file:
                    with st.spinner("מנקה בסיס נתונים וטוען מחדש..."):
                        success, msg = refresh_database_from_excel(uploaded_file)
                        if success:
                            st.success(msg)
                            st.balloons()
                            st.rerun()
                        else:
                            st.error(msg)
                else:
                    st.warning("נא לבחור קובץ תחילה.")

            st.divider()
            if st.button("🗑️ מחיקת בסיס נתונים לצמיתות"):
                if os.path.exists(DB_FILE):
                    os.remove(DB_FILE)
                    st.cache_data.clear()
                    st.success("קובץ ה-Database נמחק. המערכת ריקה.")
                    st.rerun()

if __name__ == "__main__":
    main()
