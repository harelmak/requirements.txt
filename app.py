# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v8.9.6 (MULTIMEDIA FIX)
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

# עיצוב RTL ושיפור נראות הטבלה
st.markdown("""
<style>
    html, body, [class*='css'] { direction: rtl; text-align: right; }
    .stDataFrame { border: 1px solid #ff4b4b; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# =========================
# פונקציות ניהול נתונים
# =========================

def get_connection():
    return sqlite3.connect(DB_FILE)

def force_sync_database(uploaded_file):
    """
    מבצע ניקוי טוטאלי של בסיס הנתונים וטעינה נקייה מהאקסל
    """
    try:
        # 1. קריאת האקסל
        df = pd.read_excel(uploaded_file)
        
        # 2. ניקוי נתונים: הסרת רווחים כפולים או מיותרים שגורמים לכפילויות במולטימדיה
        # זה מוודא ש "מולטימדיה " ו-"מולטימדיה" ייחשבו כאותו דבר
        df = df.applymap(lambda x: " ".join(x.split()) if isinstance(x, str) else x)
        
        # 3. מחיקת כפילויות ברמת ה-DataFrame לפני הכניסה ל-DB
        df = df.drop_duplicates()

        with get_connection() as conn:
            # 4. מחיקה פיזית של הטבלה הקיימת (DROP)
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS sales_targets")
            conn.commit()
            
            # 5. כתיבה מחדש של הנתונים הנקיים
            df.to_sql("sales_targets", conn, if_exists="replace", index=False)
            
        # 6. ניקוי ה-Cache של Streamlit
        st.cache_data.clear()
        
        return True, f"בוצע סנכרון מלא! {len(df)} שורות נטענו בצורה נקייה."
    except Exception as e:
        return False, f"שגיאה בתהליך הסנכרון: {e}"

def load_clean_data():
    @st.cache_data
    def fetch():
        try:
            with get_connection() as conn:
                return pd.read_sql("SELECT * FROM sales_targets", conn)
        except:
            return pd.DataFrame()
    return fetch()

# =========================
# ממשק משתמש
# =========================
def check_auth():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.title("התחברות למערכת Uzeb")
        col1, col2 = st.columns(2)
        with col1:
            user = st.text_input("שם משתמש")
            pwd = st.text_input("סיסמה", type="password")
            if st.button("כניסה למערכת"):
                if user == ADMIN_USERNAME and pwd == ADMIN_PASSWORD:
                    st.session_state.authenticated = True
                    st.session_state.username = ADMIN_USERNAME
                    st.rerun()
                elif user != "" and pwd != "":
                    st.session_state.authenticated = True
                    st.session_state.username = user
                    st.rerun()
                else:
                    st.error("פרטי גישה שגויים")
        return False
    return True

def main():
    if not check_auth():
        return

    st.sidebar.subheader(f"שלום, {st.session_state.username}")
    if st.sidebar.button("יציאה מהמערכת"):
        st.session_state.authenticated = False
        st.rerun()

    is_admin = (st.session_state.username == ADMIN_USERNAME)
    
    # טאבים
    tab_list = ["📊 דאשבורד", "📋 רשימת יעדים"]
    if is_admin:
        tab_list.append("⚙️ הגדרות מנהל")
    
    tabs = st.tabs(tab_list)

    # טאב דאשבורד
    with tabs[0]:
        st.header("מצב יעדים נוכחי")
        df = load_clean_data()
        if not df.empty:
            st.info(f"מציג נתונים מעודכנים עבור {len(df)} פריטים.")
            # כאן אפשר להוסיף גרפים
        else:
            st.warning("בסיס הנתונים ריק. נא לפנות למנהל לטעינת אקסל.")

    # טאב צפייה
    with tabs[1]:
        st.header("פירוט מוצרים ויעדים")
        df = load_clean_data()
        if not df.empty:
            # הוספת תיבת סינון לחיפוש מהיר של מולטימדיה
            search = st.text_input("חיפוש מוצר (לדוגמה: מולטימדיה):")
            if search:
                df = df[df.apply(lambda row: search in str(row.values), axis=1)]
            st.dataframe(df, use_container_width=True, height=500)
        else:
            st.write("אין נתונים להצגה.")

    # טאב ניהול (ADMIN)
    if is_admin:
        with tabs[2]:
            st.header("⚙️ ממשק ניהול ובקרה")
            
            st.subheader("עדכון נתונים מאקסל")
            st.markdown("""
            **הנחיות:**
            1. העלאת קובץ תמחוק את כל המידע הקיים בטבלה.
            2. המערכת תנקה כפילויות ורווחים מיותרים באופן אוטומטי.
            """)
            
            file = st.file_uploader("בחר קובץ XLSX", type=["xlsx"])
            if st.button("🔥 בצע דריסה ועדכון נתונים"):
                if file:
                    with st.spinner("מנקה בסיס נתונים וטוען מחדש..."):
                        success, msg = force_sync_database(file)
                        if success:
                            st.success(msg)
                            st.balloons()
                            st.rerun()
                        else:
                            st.error(msg)
                else:
                    st.error("חובה לבחור קובץ אקסל.")
            
            st.divider()
            if st.button("❌ מחיקת כל הנתונים (Reset)"):
                if os.path.exists(DB_FILE):
                    os.remove(DB_FILE)
                    st.cache_data.clear()
                    st.success("בסיס הנתונים נמחק פיזית.")
                    st.rerun()

if __name__ == "__main__":
    main()
