import pandas as pd
from datetime import datetime
import os
import json
from src.common.common import measure_time, fetch_api_data, save_to_csv, save_to_postgresql

@measure_time
def fetch_region_data(cortarNo, host, path, headers, sleep_seconds):
    """
    지역 데이터를 API로부터 가져오는 함수.
    
    파라미터:
    - cortarNo (str): 지역 코드.
    - host (str): API 호스트 주소.
    - path (str): API 경로.
    - headers (dict): API 호출에 사용될 헤더.
    - sleep_seconds (float): API 호출 전 대기 시간.
    
    반환값:
    - DataFrame: 지역 데이터 프레임.
    """
    params = {"cortarNo": cortarNo, "mycortarNo": ""}
    json_obj = fetch_api_data(host, path, params, headers, sleep_seconds=sleep_seconds)
    return pd.DataFrame(json_obj.get('result', {}).get('list', []))

@measure_time
def process_region(level, cortarNo, parent_info, host, path, headers, sleep_seconds):
    """
    재귀적으로 지역 데이터를 처리하는 함수.
    
    파라미터:
    - level (str): 현재 처리 중인 지역 레벨 ('city', 'dvsn', 'sec').
    - cortarNo (str): 현재 지역 코드.
    - parent_info (dict): 상위 지역 정보.
    - host (str): API 호스트 주소.
    - path (str): API 경로.
    - headers (dict): API 호출에 사용될 헤더.
    - sleep_seconds (float): API 호출 전 대기 시간.
    
    반환값:
    - list: 지역 데이터 리스트.
    """
    result = []
    df_region = fetch_region_data(cortarNo, host, path, headers, sleep_seconds)
    if df_region.empty:
        return result

    if level == 'city':
        for _, city in df_region.iterrows():
            result.extend(process_region('dvsn', city['CortarNo'], {'cityNo': city['CortarNo'], 'cityNm': city['CortarNm']}, host, path, headers, sleep_seconds))
    elif level == 'dvsn':
        for _, dvsn in df_region.iterrows():
            result.extend(process_region('sec', dvsn['CortarNo'], {**parent_info, 'dvsnNo': dvsn['CortarNo'], 'dvsnNm': dvsn['CortarNm']}, host, path, headers, sleep_seconds))
    elif level == 'sec':
        for _, sec in df_region.iterrows():
            parent_info.update({'secNo': sec['CortarNo'], 'secNm': sec['CortarNm']})
            result.append(parent_info.copy())
    return result

@measure_time
def gather_region_data(host, path, headers, work_dir, sleep_seconds, db_config=None):
    """
    지역 데이터를 수집하거나 파일에서 불러오는 함수.
    
    파라미터:
    - host (str): API 호스트 주소.
    - path (str): API 경로.
    - headers (dict): API 호출에 사용될 헤더.
    - work_dir (str): 데이터 파일을 저장할 작업 디렉터리.
    - sleep_seconds (float): API 호출 전 대기 시간.
    - db_config (dict, optional): PostgreSQL 데이터베이스 연결 설정 정보.
    
    반환값:
    - DataFrame: 지역 데이터 프레임.
    """
    now = datetime.now()
    ymd_hms = now.strftime("%Y%m%d%H%M%S")
    data_file_path = os.path.join(work_dir, f'region_data_{ymd_hms}.csv')

    if os.path.exists(data_file_path):
        df = pd.read_csv(data_file_path, dtype=str)
    else:
        final_data = process_region('city', '0000000000', {}, host, path, headers, sleep_seconds)
        df = pd.DataFrame(final_data, columns=['cityNo', 'cityNm', 'dvsnNo', 'dvsnNm', 'secNo', 'secNm'])
        df = df.astype('string')
        save_to_csv(df, data_file_path)
        if db_config:
            save_to_postgresql(df, db_config)
    return df

if __name__ == "__main__":
    # 설정값 정의
    HOME_DIR = '/Users/jodongik/workspace/prj-naver-api'
    WORK_DIR = os.path.join(HOME_DIR, 'data/resource')
    API_HOST = "m.land.naver.com"
    API_PATH = "/map/getRegionList"
    HEADERS = {'Cookie': 'NID_SES=123'}
    TIME_SLEEP_SECONDS = 2.8122
    
    # DB 설정 파일 로드
    with open(os.path.join(HOME_DIR, 'config/db_config.json')) as f:
        DB_CONFIG = json.load(f)
    
    # 데이터 수집 실행
    df = gather_region_data(API_HOST, API_PATH, HEADERS, WORK_DIR, TIME_SLEEP_SECONDS, DB_CONFIG)
    print(df)
