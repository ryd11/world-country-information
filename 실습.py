from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import logging
import json


# Redshift 연결 정보 가져오기
def get_redshift_connection():
    hook = PostgresHook(postgres_conn_id="redshift_conn_id")
    return hook.get_conn().cursor()


# 국가 정보 추출 및 Redshift 저장 함수
@task(task_id='extract_load_countries')
def extract_and_load_countries(schema, table):
    url = "https://restcountries.com/v3.1/all"
    response = requests.get(url)
    response.raise_for_status()  # 에러 발생 시 예외 처리

    countries = response.json()

    redshift_cursor = get_redshift_connection()

    # Redshift 테이블 생성 (필요한 경우)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        country VARCHAR(255),
        population BIGINT,
        area FLOAT
    );
    """
    redshift_cursor.execute(create_table_sql)

    # 기존 데이터 삭제
    redshift_cursor.execute(f"DELETE FROM {schema}.{table};")

    # 데이터 삽입
    insert_sql = f"""
    INSERT INTO {schema}.{table} (country, population, area)
    VALUES (%s, %s, %s)
    """
    for country in countries:
        country_data = (
            country['name']['official'],
            country['population'],
            country['area']
        )
        redshift_cursor.execute(insert_sql, country_data)

    # 커밋 및 연결 종료
    redshift_cursor.execute("COMMIT;")
    redshift_cursor.close()
    logging.info("Load to Redshift completed successfully.")

# DAG 생성
with DAG(
    dag_id="restcountries_to_redshift",
    start_date=datetime(2024, 1, 1),
    schedule_interval="30 6 * * 6", 
    catchup=False,
    max_active_runs=1
) as dag:

    extract_load_task = extract_and_load_countries(
        schema="your_schema_name",
        table="countries"
    )