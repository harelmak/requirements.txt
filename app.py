# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v9.0.0 (DYNAMIC FILTERING)
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

# עיצוב RTL
st.markdown("<style>html, body, [class*='css'] { direction: rtl; text-align: right; }</style>", unsafe_allow_html=True)

def get_connection():
    return sqlite3.connect(DB_FILE)

# פונקציה לטעינת נתונים (ללא Cache כדי למנוע בעיות סנכרון)
def load_data_from_db():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame()
    with get_connection() as conn:
        try:
            return pd.read_sql("SELECT * FROM sales_targets", conn)
        except:
            return pd.DataFrame()

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

    st.title("ניהול יעדי מכירות")
    
    # טעינת הנתונים
    df = load_data_from_db()

    if df.empty:
        st.warning("אין נתונים במערכת. אנא העלה קובץ אקסל בטאב המנהל.")
    else:
        # יצירת סרגל צדדי או תיבה עליונה לסינון
        st.subheader("🔍 סינון לפי קבוצה")
        
        # כאן אנחנו מניחים שעמודת קבוצת המיון נקראת "קבוצת מיון" 
        # (אם השם באקסל שלך שונה, החלף את המחרוזת 'קבוצת מיון' בשם הנכון)
        column_name = "קבוצת מיון" if "קבוצת מיון" in df.columns else df.columns[0]
        
        # הוצאת רשימת הקבוצות הייחודיות
        categories = sorted(df[column_name].unique().tolist())
        
        # תיבת בחירה
        selected_category = st.selectbox("בחר קבוצת מיון להצגה:", ["הצג הכל"] + categories)

        # פילטור הנתונים
        if selected_category != "הצג הכל":
            filtered_df = df[df[column_name] == selected_category]
        else:
            filtered_df = df

        # הצגת הטבלה המסוננת בלבד
        st.write(f"מציג {len(filtered_df)} שורות עבור: **{selected_category}**")
        st.dataframe(filtered_df, use_container_width=True, height=600)

    # --- טאב ניהול (מוסתר בתחתית או בטאב נפרד) ---
    with st.expander("⚙️ הגדרות מנהל (טעינת אקסל)"):
        f = st.file_uploader("העלה אקסל חדש (דריסת נתונים)", type=["xlsx"])
        if st.button("בצע עדכון"):
            if f:
                new_df = pd.read_excel(f)
                with get_connection() as conn:
                    new_df.to_sql("sales_targets", conn, if_exists="replace", index=False)
                st.success("הנתונים עודכנו!")
                st.rerun()

if __name__ == "__main__":
    main()
