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
DB_PATH = "mangobardata.db"
DB_PATH = os.path.join(LOCAL_DIR, "mangobardata.db")
DATE_PATH = os.path.join(LOCAL_DIR, "last_download_date.txt")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1dEy0asPIHiVeAndwHhORh-qSG1xCihIKegAtAI0lREM"
JSON_KEYFILE = os.path.join(LOCAL_DIR, "455003-8188f161c386.json")
UPLOAD_DATE = datetime.today().strftime("%Y.%m.%d")

####인증관련 함수####


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

            # ✅ 1. API 키가 같으면 무조건 통과
            if api_key == sheet_api_key:
                return True

            # ✅ 2. 아직 사용되지 않은 키면 등록
            if used.lower() == "no":
                ws.update_cell(row_idx, 2, "used")     # 'used' 상태로
                #ws.update_cell(row_idx, 3, ip)         # IP 기록
                ws.update_cell(row_idx, 4, api_key)    # API 키 저장
                return True

            # ✅ 3. API 키 다르고 used == 'used'면 실패
            return False

    return False  # licensekey 자체가 없음

def get_api_key_from_sheet(license_key):
    ws = get_worksheet()
    values = ws.get_all_values()
    df = pd.DataFrame(values[1:], columns=values[0])

    for i, row in df.iterrows():
        key = row.get("licensekey", "").strip()
        if key == license_key:
            return row.get("api_key", "").strip()  # 구글시트에 API 키 저장된 열명 확인 필요
    return None

def update_last_access(license_id):
    ws = get_worksheet()
    values = ws.get_all_values()
    df = pd.DataFrame(values[1:], columns=values[0])

    for i, row in df.iterrows():
        key = row.get("licensekey", "").strip()
        if key == license_id:
            row_idx = i + 2  # 구글시트는 1부터, 헤더 1행 있으므로 +2
            now_str = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")
            ws.update_cell(row_idx, 3, now_str)  # 3열(last_access) 업데이트
            return True
    return False

#####다운로드 함수#####

def download_json_file():
    gdrive_file_id = "19hyudWgU62umRO8-3m3LCRZCOP3BhkOe"
    import gdown
    gdown.download(f"https://drive.google.com/uc?id={gdrive_file_id}", JSON_KEYFILE, quiet=False)
    if not os.path.exists(JSON_KEYFILE) or os.path.getsize(JSON_KEYFILE) == 0:
        raise RuntimeError("인증키 다운로드 실패")

def download_db():
    gdrive_file_id = "1ZEvd4Dc6eZkHL87BYxVNNiXfZC1YUuV1"
    onedrive_url = "https://api.onedrive.com/v1.0/shares/s!AvF2hXhg7zrHix0kLOdKcvSLF0U0/root/content"

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    try:
        gdown.download(f"https://drive.google.com/uc?id={gdrive_file_id}", DB_PATH, quiet=False)
        if not os.path.exists(DB_PATH) or os.path.getsize(DB_PATH) == 0:
            raise Exception("gdown 파일 없음")
    except Exception as e:

        try:
            r = requests.get(onedrive_url)
            with open(DB_PATH, "wb") as f:
                f.write(r.content)

        except Exception as ex:

            raise ex

    # 성공 메시지 잠깐 보여주기
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
        modified_time = file["modifiedTime"]  # 예: '2025-07-28T06:30:00.000Z'

        dt = datetime.strptime(modified_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        # 영어 월 이름 + 숫자 + 연도 + 시분으로 포맷팅
        return dt.strftime("%b %d, %Y %H:%M")
    except Exception as e:
        return "불러오기 실패"




#####데이터 로드 함수#####

# ===== 📋 데이터 불러오기 =====
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


#더블클릭시 변경정보 호출#
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


#### 검색보조함수-글자분해검색#
def contains_all_chars(df, query):
    query_chars = list(query)  # 검색어 글자를 하나씩 분해
    matched_indices = []

    for idx, row in df.iterrows():
        name = row.get("_BSSH_NORM", "")
        if all(char in name for char in query_chars):
            matched_indices.append(idx)

    return df.loc[matched_indices]


####테이블 렌더링 함수
def show_table_simple(df):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_selection(selection_mode="none")  # 클릭 비활성
    gb.configure_grid_options(domLayout='normal')

    # 컬럼 너비 자동 혹은 임의 지정 (영업/정상과 비슷하게)

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


# 클립보드 이벤트 감지 + 복사됨 알림 JS 삽입
components.html("""
    <script>
    document.addEventListener('copy', function() {
        let toast = document.createElement('div');
        toast.innerText = '복사되었습니다';
        toast.style.position = 'fixed';
        toast.style.top = '20px';
        toast.style.right = '20px';
        toast.style.background = '#333';
        toast.style.color = 'white';
        toast.style.padding = '10px 15px';
        toast.style.borderRadius = '8px';
        toast.style.boxShadow = '0 2px 6px rgba(0,0,0,0.2)';
        toast.style.zIndex = '9999';
        document.body.appendChild(toast);

        setTimeout(() => {
            toast.remove();
        }, 1500); // 1.5초 뒤 사라짐
    });
    </script>
""", height=0)

def show_table_change_info_only(df, key=None):
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_selection(selection_mode="none")
    gb.configure_grid_options(domLayout='normal', enableCellTextSelection=True)

    gridOptions = gb.build()

    js = """
    function onCellDoubleClicked(params) {
        const text = params.value;
        if (text) {
            navigator.clipboard.writeText(text);
            // Streamlit에 복사 알림 신호 전달
            window.parent.postMessage({isCopy:true}, "*");
        }
    }
    """

    gridOptions['onCellDoubleClicked'] = "function(params) {" + js + "onCellDoubleClicked(params);}"

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
    gb.configure_grid_options(domLayout='normal')


    gridOptions = gb.build()
    grid_response = AgGrid(
        df,
        gridOptions=gridOptions,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=False,
        height=400,        # 적당한 높이 지정
        width=1300         # 가로 고정 1300px
    )
    

    selected = grid_response.get('selected_rows', None)
    if selected is not None:
        if selected is not None:
            if (hasattr(selected, 'empty') and not selected.empty) or (not hasattr(selected, 'empty') and len(selected) > 0):
                first_item = selected.iloc[0] if hasattr(selected, 'iloc') else selected[0]
                lcns_no = first_item['인허가번호']
        
                # 사용자 라이선스 ID 세션에서 가져오기
                license_id = st.session_state.get("license_id", None)
                if license_id:
                    api_key = get_api_key_from_sheet(license_id)
                    if api_key:
                        change_info = fetch_change_info(api_key, lcns_no)
                    else:
                        change_info = None
                else:
                    change_info = None
        
                if change_info:
                    st.write("### 변경 정보")
                    df_change = pd.DataFrame(change_info)
                    show_table_change_info_only(df_change, key="change_info_grid")
                else:
                    st.write("변경 정보가 없거나 불러올 수 없습니다.30초후에 재시도 해주세요.")

# ===== 🖥️ Streamlit 인터페이스 =====
st.set_page_config(page_title="티스토리 foofighters", layout = "wide")

def main():
    st.title("foofighters")
    drive_file_id = "1ZEvd4Dc6eZkHL87BYxVNNiXfZC1YUuV1"
    cred_path = "455003-8188f161c386.json"


    if "api_key" not in st.session_state:
        st.session_state.api_key = None
    if "has_rerun" not in st.session_state:
        st.session_state.has_rerun = False


    if st.session_state.api_key is None:
        # json 파일 없으면 다운로드 버튼 보이게
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
                    st.session_state.has_rerun = True
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





if __name__ == "__main__":
    main()














