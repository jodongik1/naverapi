import http.client
import json
import time
import pandas as pd
from datetime import datetime
from functools import wraps
import psycopg2
from psycopg2 import sql

def measure_time(func):
    """함수의 실행 시간을 측정하는 데코레이터."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        try:
            return func(*args, **kwargs)
        finally:
            end_time = datetime.now()
            duration = end_time - start_time
            print(f"{func.__name__} 처리 시간: {duration}")
    return wrapper

@measure_time
def fetch_api_data(host, path, params, headers=None, method="GET", sleep_seconds=2.8122):
    """
    API를 호출하여 데이터를 가져오는 함수.
    
    파라미터:
    - host (str): API 호스트 주소.
    - path (str): API 경로.
    - params (dict): API 호출에 사용될 파라미터.
    - headers (dict, optional): API 호출에 사용될 헤더. 기본값은 None.
    - method (str, optional): HTTP 메소드 (기본값: "GET").
    - sleep_seconds (float, optional): API 호출 전 대기 시간 (기본값: 2.8122초).
    
    반환값:
    - dict: API 응답 데이터.
    """
    try:
        time.sleep(sleep_seconds)
        conn = http.client.HTTPSConnection(host)
        url_path = path + '?' + '&'.join([f"{key}={value}" for key, value in params.items()])
        conn.request(method, url_path, '', headers or {})
        res = conn.getresponse()
        data = res.read()
        return json.loads(data.decode("utf-8"))
    except (http.client.HTTPException, json.JSONDecodeError) as e:
        print(f"Error occurred: {e}")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
    return {}

def save_to_csv(df, file_path):
    """
    데이터를 CSV 파일로 저장하는 함수.
    
    파라미터:
    - df (DataFrame): 저장할 데이터 프레임.
    - file_path (str): 저장할 파일 경로.
    """
    df.to_csv(file_path, index=False)

def save_to_postgresql(df, db_config):
    """
    데이터를 PostgreSQL 데이터베이스에 저장하는 함수.
    
    파라미터:
    - df (DataFrame): 저장할 데이터 프레임.
    - db_config (dict): 데이터베이스 연결 설정 정보.
    """
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        table_name = "region_data"
        columns = df.columns

        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                cityNo VARCHAR(50),
                cityNm VARCHAR(100),
                dvsnNo VARCHAR(50),
                dvsnNm VARCHAR(100),
                secNo VARCHAR(50),
                secNm VARCHAR(100)
            )
        """).format(sql.Identifier(table_name))
        cur.execute(create_table_query)

        insert_query = sql.SQL("""
            INSERT INTO {} ({})
            VALUES ({})
        """).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        for record in df.itertuples(index=False):
            cur.execute(insert_query, record)

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error occurred while saving to PostgreSQL: {e}")
