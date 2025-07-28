import streamlit as st
import xml.etree.ElementTree as ET
import time
import requests
import sqlite3
import gdown
import pandas as pd
from rapidfuzz import fuzz
import re
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ===== 🔐 구글 시트 기반 인증 함수 =====
SHEET_URL = "https://docs.google.com/spreadsheets/d/1dEy0asPIHiVeAndwHhORh-qSG1xCihIKegAtAI0lREM"
JSON_KEYFILE = "455003-8188f161c386.json"

def get_ip():
    return requests.get("https://api.ipify.org").text.strip()

def get_worksheet():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE, scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL).sheet1

def check_license_with_ip_and_key(license_key, api_key):
    ip = get_ip()
    ws = get_worksheet()
    records = ws.get_all_records()

    for i, row in enumerate(records):
        key = row.get("licensekey", "").strip()
        stored_api_key = row.get("api_key", "").strip()
        used = row.get("used", "").strip().lower()
        ip_in_sheet = row.get("ip_address", "").strip()

        if key == license_key:
            row_idx = i + 2  # 구글시트는 1행이 헤더이므로 +2
            if used == "no":
                ws.update_cell(row_idx, 2, "used")       # used 열 업데이트
                ws.update_cell(row_idx, 3, ip)           # ip_address 열 업데이트
                ws.update_cell(row_idx, 4, api_key)      # api_key 열 업데이트
                return True
            elif used == "used" and ip == ip_in_sheet and stored_api_key == api_key:
                return True
            else:
                return False
    return False


# ===== 📦 DB 자동 다운로드 =====
PAGE_SIZE = 200
DB_PATH = "mangobardata.db"

def download_db():
    file_id = "1cjYTpM40hMOs817KvSOWq1HmLkvUdCXn"
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    gdown.download(f"https://drive.google.com/uc?id={file_id}", DB_PATH, quiet=False)

download_db()  # 앱 시작 시 최신 DB 다운로드

# ===== 📋 데이터 불러오기 =====
def load_data(selected_regions, query_addr, query_bssh, page=1):
    offset = (page - 1) * PAGE_SIZE
    conn = sqlite3.connect(DB_PATH)

    region_clauses = []
    for region in selected_regions:
        prefix = region[:4].lower()
        region_clauses.append(f"_ADDR_LOWER LIKE '{prefix}%'")
    region_condition = " OR ".join(region_clauses) if region_clauses else "1=1"

    query_addr = query_addr.lower() if query_addr else ""
    query_bssh_norm = query_bssh.replace(" ", "").lower() if query_bssh else ""

    sql_i2500 = f"""
        SELECT LCNS_NO, INDUTY_CD_NM, BSSH_NM, ADDR, PRMS_DT
        FROM i2500
        WHERE ({region_condition})
        AND _ADDR_LOWER LIKE ?
        AND _BSSH_NORM LIKE ?
    """

    sql_i2819 = f"""
        SELECT LCNS_NO, INDUTY_NM, BSSH_NM, LOCP_ADDR, PRMS_DT, CLSBIZ_DT, CLSBIZ_DVS_CD_NM
        FROM i2819
        WHERE ({region_condition})
        AND _ADDR_LOWER LIKE ?
        AND _BSSH_NORM LIKE ?
    """

    params = (f"%{query_addr}%", f"%{query_bssh_norm}%")

    df_i2500 = pd.read_sql_query(sql_i2500, conn, params=params)
    df_i2819 = pd.read_sql_query(sql_i2819, conn, params=params)

    conn.close()

    df_i2500_display = df_i2500.rename(columns={
        "LCNS_NO": "인허가번호",
        "INDUTY_CD_NM": "업종",
        "BSSH_NM": "업소명",
        "ADDR": "주소",
        "PRMS_DT": "허가일자",
    })

    df_i2819_display = df_i2819.rename(columns={
        "LCNS_NO": "인허가번호",
        "INDUTY_NM": "업종",
        "BSSH_NM": "업소명",
        "LOCP_ADDR": "주소",
        "PRMS_DT": "허가일자",
        "CLSBIZ_DT": "폐업일자",
        "CLSBIZ_DVS_CD_NM": "폐업상태",
    })

    df_i2500_display["_BSSH_NORM"] = df_i2500_display["업소명"].fillna("").str.replace(" ", "").str.lower()
    df_i2819_display["_BSSH_NORM"] = df_i2819_display["업소명"].fillna("").str.replace(" ", "").str.lower()

    return df_i2500_display, df_i2819_display

# ===== 🔎 유사도 검색 =====
def fuzzy_search(df, query, threshold=75):
    query_norm = query.replace(" ", "").lower()
    results = []
    for idx, row in df.iterrows():
        name = row["_BSSH_NORM"]
        score = fuzz.token_set_ratio(query_norm, name)
        if score >= threshold:
            results.append(idx)
    return df.loc[results]

# ===== 🖥️ Streamlit 인터페이스 =====
st.set_page_config(page_title="MangoBar 웹 검색", layout="wide")

def main():
    st.title("웹 검색")

    if "api_key" not in st.session_state:
        st.session_state.api_key = None
    if "has_rerun" not in st.session_state:
        st.session_state.has_rerun = False

    if st.session_state.api_key is None:
        with st.form("api_key_form"):
            license_id = st.text_input("라이센스 ID 입력")
            api_key = st.text_input("식품안전나라 인증키 입력", type="password")
            submit = st.form_submit_button("인증")

        if submit:
            license_id = license_id.strip()
            api_key = api_key.strip()
            if license_id and api_key:
                if check_license_with_ip_and_key(license_id, api_key):
                    st.session_state.api_key = api_key
                    st.session_state.license_id = license_id
                    st.experimental_rerun()
                else:
                    st.warning("인증 실패: ID 또는 인증키가 틀렸거나 이미 사용된 키입니다.")
            else:
                st.warning("ID와 인증키를 모두 입력해주세요.")
        return


    with st.form("search_form"):
        selected_regions = st.multiselect("시·도를 선택하세요", options=[
            "서울특별시", "경기도", "인천광역시", "세종특별자치시", "부산광역시",
            "대구광역시", "광주광역시", "대전광역시", "울산광역시",
            "강원특별자치도", "충청북도", "충청남도",
            "전북특별자치도", "전라남도", "경상북도", "경상남도", "제주특별자치도"
        ])
        query_addr = st.text_input("주소를 입력하세요").strip().lower()
        query_bssh = st.text_input("업소명을 입력하세요").strip().replace(" ", "").lower()

        search_submitted = st.form_submit_button("검색")

    if search_submitted:
        if not selected_regions:
            st.warning("최소 하나의 시·도를 선택하세요.")
            return
        if not query_addr and not query_bssh:
            st.warning("주소 또는 업소명을 입력하세요.")
            return

        df_i2500, df_i2819 = load_data(selected_regions, query_addr, query_bssh, page=1)

        if query_bssh:
            query_words = re.findall(r'\w+', query_bssh.lower())
            mask_unordered_2500 = df_i2500['_BSSH_NORM'].apply(lambda x: all(word in x for word in query_words))
            df_i2500_filtered = df_i2500[mask_unordered_2500]
            df_i2500_filtered = fuzzy_search(df_i2500_filtered, query_bssh, threshold=80)
            df_i2500 = df_i2500_filtered

            mask_unordered_2819 = df_i2819['_BSSH_NORM'].apply(lambda x: all(word in x for word in query_words))
            df_i2819_filtered = df_i2819[mask_unordered_2819]
            df_i2819_filtered = fuzzy_search(df_i2819_filtered, query_bssh, threshold=80)
            df_i2819 = df_i2819_filtered

        st.success(f"검색 완료: 정상 {len(df_i2500)}개 / 폐업 {len(df_i2819)}개")
        st.write("### 영업/정상")
        st.dataframe(df_i2500.drop(columns=["_BSSH_NORM", "_BSSH_LOWER"], errors='ignore'), use_container_width=True)

        st.write("### 폐업")
        st.dataframe(df_i2819.drop(columns=["_BSSH_NORM", "_BSSH_LOWER"], errors='ignore'), use_container_width=True)


###더블클릭시 변경정보 호출##
import xml.etree.ElementTree as ET

def fetch_change_info(api_key, lcns_no):
    url = f"http://openapi.foodsafetykorea.go.kr/api/{api_key}/I2861/xml/1/500/LCNS_NO={lcns_no}"
    resp = requests.get(url)
    if resp.status_code != 200:
        return None

    root = ET.fromstring(resp.content)
    items = root.findall('.//row')
    results = []
    for item in items:
        before = item.findtext('CHNG_BF_CN', default='').strip()
        after = item.findtext('CHNG_AF_CN', default='').strip()
        date = item.findtext('CHNG_DT', default='').strip()
        if len(date) == 8:
            date = f"{date[:4]}-{date[4:6]}-{date[6:]}"
        results.append({
            "변경 전 내용": before,
            "변경 후 내용": after,
            "변경일자": date
        })
    return results

from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

def show_table_with_click(df):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_selection(selection_mode="single", use_checkbox=False)
    gb.configure_grid_options(domLayout='normal')
    gridOptions = gb.build()

    grid_response = AgGrid(df, gridOptions=gridOptions,
                          update_mode=GridUpdateMode.SELECTION_CHANGED,
                          allow_unsafe_jscode=True)

    selected = grid_response['selected_rows']
    if selected:
        lcns_no = selected[0]['인허가번호']
        change_info = fetch_change_info(st.session_state.api_key, lcns_no)
        if change_info:
            st.write("### 변경 정보")
            for line in change_info:
                st.write(line)
        else:
            st.write("변경 정보를 불러올 수 없습니다.")


if __name__ == "__main__":
    main()
