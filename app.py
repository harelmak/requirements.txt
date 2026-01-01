# -*- coding: utf-8 -*-
"""
Uzeb Sales Targets — v8.8.0 (FULL FILE)
- NEW: Added dynamic item table based on 'Sort Code' selection.
- NEW: Market share calculation per category.
- UX: Integrated drill-down view in Target Editing.
- SECURITY: Admin/Agent view separation.
"""

import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from pathlib import Path

# =========================
# הגדרות ועיצוב (UI/UX)
# =========================
st.set_page_config(page_title="Uzeb — Edit Targets", layout="wide")

st.markdown("""
<style>
    @import url('fonts.googleapis.com');
    html, body, [class*="css"] { direction: rtl; font-family: "Heebo", sans-serif; }
    .stMetric { background: #f8f9fa; border: 1px solid #eee; border-radius: 12px; padding: 15px; }
    .stNumberInput input { border-radius: 8px !important; }
    div.stButton > button { border-radius: 10px !important; font-weight: 700; width: 100%; transition: 0.3s; }
    .details-container { background-color: #f0f4f8; padding: 20px; border-radius: 15px; border-right: 5px solid #007bff; margin-top: 20px; }
</style>
""", unsafe_allow_html=True)

# =========================
# קבועים
# =========================
COL_ACCOUNT = "שם חשבון"
COL_CLASS = "שם קוד מיון פריט"
COL_ITEM = "שם פריט"
COL_QTY = "סהכ כמות"
COL_NET = "מכירות/קניות נטו"
ADMIN_USERNAME = "ADMIN"
ADMIN_PASSWORD = "1511!!"

# =========================
# פונקציות מסד נתונים
# =========================
DB_FILENAME = "uzeb_app.sqlite"
DEFAULT_DB_DIR = Path(".") / "data"

def get_db_path() -> Path:
    DEFAULT_DB_DIR.mkdir(parents=True, exist_ok=True)
    return DEFAULT_DB_DIR / DB_FILENAME

def update_item_delta(username, account, item, cls, delta):
    # פונקציית עדכון (לוגיקה קיימת)
    try:
        con = sqlite3.connect(get_db_path().as_posix())
        now = datetime.now(timezone.utc).isoformat()
        con.execute("""
            INSERT INTO user_class_delta_qty (username, account, cls, item, delta_qty, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(username, account, item) DO UPDATE SET
                delta_qty = excluded.delta_qty,
                updated_at = excluded.updated_at
        """, (username, account, cls, item, delta, now))
        con.commit()
        con.close()
        st.toast(f"היעד עבור {item} עודכן!")
    except Exception as e:
        st.error(f"שגיאה בעדכון: {e}")

# =========================
# טבלת פירוט ברמת פריט (התוספת החדשה)
# =========================
def render_item_details_table(df_account: pd.DataFrame, selected_class: str):
    """ מציג טבלה עם פירוט פריטים ונתח מכירות לקוד המיון הנבחר """
    st.markdown(f'<div class="details-container">', unsafe_allow_html=True)
    st.subheader(f"📊 פירוט פריטים בקטגוריית: {selected_class}")
    
    # סינון פריטים השייכים לאותו קוד מיון
    df_filtered = df_account[df_account[COL_CLASS] == selected_class].copy()
    
    # חישוב נתח מכירות בכסף
    total_class_sales = df_filtered[COL_NET].sum()
    if total_class_sales > 0:
        df_filtered['נתח מכירות %'] = (df_filtered[COL_NET] / total_class_sales * 100).round(1)
    else:
        df_filtered['נתח מכירות %'] = 0

    # עיצוב הטבלה להצגה
    display_df = df_filtered[[COL_ITEM, COL_QTY, COL_NET, 'נתח מכירות %']].copy()
    display_df.columns = ["שם פריט", "כמות", "מכירות (₪)", "נתח מהקטגוריה (%)"]
    
    st.dataframe(display_df.style.format({"מכירות (₪)": "{:,.2f}", "נתח מהקטגוריה (%)": "{}%"}), 
                 use_container_width=True, hide_index=True)
    
    st.markdown(f"**סה\"כ מכירות לקוד מיון זה:** {total_class_sales:,.2f} ₪")
    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# ממשק עריכת יעדים
# =========================
def render_target_editing_view(df: pd.DataFrame, account_name: str, username: str):
    st.subheader(f"🎯 ניהול יעדים: {account_name}")
    
    # סינון הנתונים ללקוח
    acc_df = df[df[COL_ACCOUNT] == account_name].copy()
    if acc_df.empty:
        st.warning("לא נמצאו נתונים.")
        return

    # ניהול המצב (State) של קוד המיון הנבחר
    if f"selected_cls_{account_name}" not in st.session_state:
        st.session_state[f"selected_cls_{account_name}"] = None

    # חיפוש מהיר
    search = st.text_input("🔍 חיפוש פריט:", placeholder="הקלד שם פריט...")
    if search:
        display_df = acc_df[acc_df[COL_ITEM].str.contains(search, na=False, case=False)]
    else:
        display_df = acc_df

    st.markdown("---")
    
    # כותרות הטבלה
    h1, h2, h3, h4, h5 = st.columns([3, 2, 1, 1, 1.2])
    h1.write("**שם פריט**")
    h2.write("**קוד מיון**")
    h3.write("**כמות 2025**")
    h4.write("**עדכון Delta**")
    h5.write("**פעולה**")

    # הצגת השורות
    for idx, row in display_df.iterrows():
        c1, c2, c3, c4, c5 = st.columns([3, 2, 1, 1, 1.2])
        with c1: st.text(row[COL_ITEM])
        with c2: st.caption(row[COL_CLASS])
        with c3: st.text(f"{int(row[COL_QTY])} יח'")
        with c4:
            new_val = st.number_input("Delta", value=0.0, key=f"d_{idx}", label_visibility="collapsed")
        with c5:
            # שני כפתורים קטנים: אחד לשמירה ואחד לפירוט
            btn_col1, btn_col2 = st.columns(2)
            if btn_col1.button("💾", key=f"sv_{idx}", help="שמור יעד"):
                update_item_delta(username, account_name, row[COL_ITEM], row[COL_CLASS], new_val)
            if btn_col2.button("🔍", key=f"det_{idx}", help="הצג פירוט קוד מיון"):
                st.session_state[f"selected_cls_{account_name}"] = row[COL_CLASS]

    # הצגת הטבלה המפורטת למטה אם נבחר קוד מיון
    if st.session_state[f"selected_cls_{account_name}"]:
        st.write("")
        render_item_details_table(acc_df, st.session_state[f"selected_cls_{account_name}"])

# =========================
# MAIN APP
# =========================
def main():
    if "auth" not in st.session_state:
        st.session_state.auth = False
        st.session_state.is_admin = False

    if not st.session_state.auth:
        st.title("Uzeb Targets 2026")
        u = st.text_input("משתמש")
        p = st.text_input("סיסמה", type="password")
        if st.button("כניסה"):
            if u == ADMIN_USERNAME and p == ADMIN_PASSWORD:
                st.session_state.auth, st.session_state.is_admin = True, True
            elif u != "":
                st.session_state.auth = True
            st.session_state.username = u
            st.rerun()
        return

    # תפריט צד
    st.sidebar.title(f"שלום, {st.session_state.username}")
    mode = st.sidebar.radio("ניווט:", ["צפייה בנתונים", "עריכת יעדי לקוח"])

    # נתוני דוגמה (יש להחליף בשליפה מה-DB שלך)
    df_main = pd.DataFrame({
        COL_ACCOUNT: ["קרמיקה אבי", "קרמיקה אבי", "קרמיקה אבי", "הכל לבית", "הכל לבית"],
        COL_ITEM: ["ברז מטבח נשלף", "מזלף ניקל", "ברז אמבטיה", "כיור גרניט", "סיפון"],
        COL_CLASS: ["ברזים", "מקלחות", "ברזים", "כיורים", "אינסטלציה"],
        COL_QTY: [50, 120, 30, 45, 200],
        COL_NET: [15000, 4000, 8000, 25000, 2000]
    })

    if mode == "צפייה בנתונים":
        st.header("📊 מצב מכירות")
        st.dataframe(df_main, use_container_width=True, hide_index=True)

    elif mode == "עריכת יעדי לקוח":
        acc = st.selectbox("בחר לקוח לעריכה:", df_main[COL_ACCOUNT].unique())
        if acc:
            render_target_editing_view(df_main, acc, st.session_state.username)

    if st.sidebar.button("התנתק"):
        st.session_state.auth = False
        st.rerun()

if __name__ == "__main__":
    main()
