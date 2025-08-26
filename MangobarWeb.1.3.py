import streamlit as st
import xml.etree.ElementTree as ET
import time
from datetime import datetime, timedelta
import requests
import sqlite3
import gdown
import pandas as pd
from rapidfuzz import fuzz
import re
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from google.oauth2 import service_account
from googleapiclient.discovery import build
import streamlit.components.v1 as components

LOCAL_DIR = os.path.expanduser("~/datadb")
os.makedirs(LOCAL_DIR, exist_ok=True)
PAGE_SIZE = 200
DB_PATH = os.path.join(LOCAL_DIR, "mangobardata.db")
DATE_PATH = os.path.join(LOCAL_DIR, "last_download_date.txt")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1dEy0asPIHiVeAndwHhORh-qSG1xCihIKegAtAI0lREM"
JSON_KEYFILE = os.path.join(LOCAL_DIR, "455003-8188f161c386.json")
UPLOAD_DATE = datetime.today().strftime("%Y.%m.%d")

##### 인증관련 함수 #####

def get_worksheet():
    if not os.path.exists(JSON_KEYFILE):
        raise FileNotFoundError("인증키 파일이 존재하지 않습니다. 다운로드 버튼을 눌러 주세요.")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE, scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL).sheet1

def check_license_with_ip_and_key(license_key, api_key):
    ws = get_worksheet()
    values = ws.get_all_values()
    df = pd.DataFrame(values[1:], columns=values[0])
    for i, row in df.iterrows():
        key = row.get("licensekey", "").strip()
        sheet_api_key = row.get("api_key", "").strip()
        used = row.get("used", "").strip()
        if key == license_key:
            row_idx = i + 2
            if api_key == sheet_api_key:
                return True
            if used.lower() == "no":
                ws.update_cell(row_idx, 2, "used")
                ws.update_cell(row_idx, 4, api_key)
                return True
            return False
    return False

def get_api_key_from_sheet(license_key):
    ws = get_worksheet()
    values = ws.get_all_values()
    df = pd.DataFrame(values[1:], columns=values[0])
    for i, row in df.iterrows():
        key = row.get("licensekey", "").strip()
        if key == license_key:
            return row.get("api_key", "").strip()
    return None

def update_last_access(license_id):
    ws = get_worksheet()
    values = ws.get_all_values()
    df = pd.DataFrame(values[1:], columns=values[0])
    for i, row in df.iterrows():
        key = row.get("licensekey", "").strip()
        if key == license_id:
            row_idx = i + 2
            now_str = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")
            ws.update_cell(row_idx, 3, now_str)
            return True
    return False

##### 다운로드 함수 #####

def download_json_file():
    gdrive_file_id = "19hyudWgU62umRO8-3m3LCRZCOP3BhkOe"
    gdown.download(f"https://drive.google.com/uc?id={gdrive_file_id}", JSON_KEYFILE, quiet=False)
    if not os.path.exists(JSON_KEYFILE) or os.path.getsize(JSON_KEYFILE) == 0:
        raise RuntimeError("인증키 다운로드 실패")

def download_db():
    gdrive_file_id = "1ZEvd4Dc6eZkHL87BYxVNNiXfZC1YUuV1"
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    gdown.download(f"https://drive.google.com/uc?id={gdrive_file_id}", DB_PATH, quiet=False)
    if not os.path.exists(DB_PATH) or os.path.getsize(DB_PATH) == 0:
        raise RuntimeError("DB 다운로드 실패")
    msg = st.empty()
    msg.success("✅ DB 다운로드 완료!")
    time.sleep(1)
    msg.empty()

def get_drive_file_modified_date(file_id, cred_path):
    try:
        scopes = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
        creds = service_account.Credentials.from_service_account_file(cred_path, scopes=scopes)
        service = build("drive", "v3", credentials=creds)
        file = service.files().get(fileId=file_id, fields="modifiedTime").execute()
        modified_time = file["modifiedTime"]
        dt = datetime.strptime(modified_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.strftime("%b %d, %Y %H:%M")
    except Exception as e:
        return "불러오기 실패"

##### 데이터 로드 #####

def load_data(selected_regions, query_addr, query_bssh, page=1):
    offset = (page - 1) * PAGE_SIZE
    conn = sqlite3.connect(DB_PATH)
    params = []
    query_addr_param = f"%{query_addr.lower()}%" if query_addr else '%'
    if selected_regions:
        region_prefixes = [region[:4].lower() for region in selected_regions]
        region_condition = " OR ".join(["_ADDR_LOWER LIKE ?"] * len(region_prefixes))
        region_condition = f"({region_condition}) AND _ADDR_LOWER LIKE ?"
        params = [f"{prefix}%" for prefix in region_prefixes]
        params.append(query_addr_param)
    else:
        region_condition = "_ADDR_LOWER LIKE ?"
        params = [query_addr_param]
    sql_i2500 = f"""
        SELECT LCNS_NO, INDUTY_CD_NM, BSSH_NM, ADDR, PRMS_DT, _BSSH_NORM
        FROM i2500
        WHERE {region_condition}
    """
    sql_i2819 = f"""
        SELECT LCNS_NO, INDUTY_NM, BSSH_NM, LOCP_ADDR, PRMS_DT, CLSBIZ_DT, CLSBIZ_DVS_CD_NM, _BSSH_NORM
        FROM i2819
        WHERE {region_condition}
    """
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
    return df_i2500_display, df_i2819_display

##### 변경정보 호출 #####

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

##### 검색보조 함수 #####

def contains_all_chars(df, query):
    query_chars = list(query)
    matched_indices = []
    for idx, row in df.iterrows():
        name = row.get("_BSSH_NORM", "")
        if all(char in name for char in query_chars):
            matched_indices.append(idx)
    return df.loc[matched_indices]

##### 테이블 렌더링 #####

def show_table_simple(df):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_selection(selection_mode="none")
    gb.configure_grid_options(domLayout='normal')
    gridOptions = gb.build()
    AgGrid(
        df,
        gridOptions=gridOptions,
        height=400,
        width=1300,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=False,
        update_mode=GridUpdateMode.NO_UPDATE,
    )

def show_table_change_info_only(df, key=None):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_selection(selection_mode="none")
    gb.configure_grid_options(domLayout='normal', enableCellTextSelection=True)
    gridOptions = gb.build()
    AgGrid(
        df,
        gridOptions=gridOptions,
        update_mode=GridUpdateMode.NO_UPDATE,
        fit_columns_on_grid_load=False,
        use_container_width=True,
        key=key,
    )

def show_table_with_click(df):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_selection(selection_mode="single", use_checkbox=False)
    gb.configure_grid_options(domLayout='normal', enableCellTextSelection=True)
    gridOptions = gb.build()

    js_double_click = """
    function onCellDoubleClicked(params) {
        const lcns_no = params.data['인허가번호'];
        if(lcns_no){
            window.parent.postMessage({lcns_no_clicked: lcns_no}, "*");
        }
    }
    """
    gridOptions['onCellDoubleClicked'] = js_double_click

    AgGrid(
        df,
        gridOptions=gridOptions,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=False,
        height=400,
        width=1300
    )

##### Streamlit 인터페이스 #####

st.set_page_config(page_title="티스토리 foofighters", layout="wide")

# JS → Python 메시지 연결 (1회만)
components.html("""
<script>
window.addEventListener("message", (event) => {
    if(event.data.lcns_no_clicked){
        const lcns_no = event.data.lcns_no_clicked;
        window.parent.postMessage({'lcns_no_clicked': lcns_no}, "*");
    }
});
</script>
""", height=0)

def main():
    st.title("foofighters")
    drive_file_id = "1ZEvd4Dc6eZkHL87BYxVNNiXfZC1YUuV1"
    cred_path = JSON_KEYFILE

    if "api_key" not in st.session_state:
        st.session_state.api_key = None

    if st.session_state.api_key is None:
        if not os.path.exists(JSON_KEYFILE):
            if st.button("인증하기"):
                try:
                    download_json_file()
                    st.success("새로고침을 눌러주세요.")
                except Exception as e:
                    st.error(f"다운로드 실패: {e}")
            st.stop()
        with st.form("api_key_form"):
            license_id = st.text_input("라이센스 ID 입력")
            api_key = st.text_input("인증키 입력", type="password")
            submit = st.form_submit_button("인증")
        if submit:
            license_id = license_id.strip()
            api_key = api_key.strip()
            if license_id and api_key:
                if check_license_with_ip_and_key(license_id, api_key):
                    update_last_access(license_id)
                    st.session_state.api_key = api_key
                    st.session_state.license_id = license_id
                    st.rerun()
                    return
                else:
                    st.warning("인증 실패: ID 또는 인증키가 틀렸거나 이미 사용된 키입니다.")
            else:
                st.warning("ID와 인증키를 모두 입력해주세요.")
        return

    date_str = get_drive_file_modified_date(drive_file_id, cred_path)
    col1, col2 = st.columns([1, 3])
    if st.button(f"{UPLOAD_DATE} 다운받기"):
        try:
            download_db()
            st.success("DB 다운로드 완료")
        except Exception as e:
            st.error(f"DB 다운로드 실패: {e}")

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
        st.session_state.selected_regions = selected_regions
        st.session_state.query_addr = query_addr
        st.session_state.query_bssh = query_bssh
        df_i2500, df_i2819 = load_data(selected_regions, query_addr, query_bssh, page=1)
        st.session_state.search_results = (df_i2500, df_i2819)
    elif "search_results" in st.session_state:
        df_i2500, df_i2819 = st.session_state.search_results
    else:
        df_i2500, df_i2819 = None, None

    if df_i2500 is not None and df_i2819 is not None:
        if query_bssh:
            df_i2500 = contains_all_chars(df_i2500, query_bssh)
            df_i2819 = contains_all_chars(df_i2819, query_bssh)
        st.success(f"검색 완료: 정상 {len(df_i2500)}개 / 폐업 {len(df_i2819)}개")
        st.write("### 영업/정상")
        show_table_with_click(df_i2500.drop(columns=["_BSSH_NORM", "_BSSH_LOWER"], errors='ignore'))
        st.write("### 폐업")
        show_table_simple(df_i2819.drop(columns=["_BSSH_NORM", "_BSSH_LOWER"], errors='ignore'))

    # 정상 테이블 더블클릭 시 변경정보 표시
    if "lcns_no_clicked" in st.session_state:
        lcns_no = st.session_state.lcns_no_clicked
        api_key = st.session_state.api_key
        if api_key:
            change_info = fetch_change_info(api_key, lcns_no)
            if change_info:
                st.write("### 변경 정보")
                df_change = pd.DataFrame(change_info)
                show_table_change_info_only(df_change)
            else:
                st.warning("변경 정보가 없거나 불러올 수 없습니다.")
        del st.session_state.lcns_no_clicked  # 한번 표시 후 삭제

if __name__ == "__main__":
    main()
