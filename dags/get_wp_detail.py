import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

url = "https://op.itmo.ru/auth/token/login"
username = Variable.get ("username")
password = Variable.get ("password")
auth_data = {"username": username, "password": password}
token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["auth_token"]
headers = {'Content-Type': "application/json", 'Authorization': "Token " + token}

def get_wp_detail():
    dt = pendulum.now("Europe/Moscow").to_iso8601_string()
    new_data = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    select 
    distinct
    (json_array_elements(wp_in_academic_plan::json)->>'id')::integer as id,
    (json_array_elements(wp_in_academic_plan::json)->>'discipline_code')::varchar(20) as discipline_code,
    (json_array_elements(wp_in_academic_plan::json)->>'title')::text as title,
    (json_array_elements(wp_in_academic_plan::json)->>'description')::text as description,
    (json_array_elements(wp_in_academic_plan::json)->>'status')::varchar(20) as status
    from stg.work_programs wp
    """)
    existing_data = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    select 'id', 'discipline_code', 'title', 'description', 'status', 'update_ts' from stg.wp_detail
    order by 1
    """)
    # url_down = 'https://op.itmo.ru/api/workprogram/detail/'
    target_fields = ['id', 'discipline_code', 'title', 'description', 'status', 'update_ts']
    df_existing_data = pd.DataFrame(existing_data, columns=target_fields)
    df_new_data = pd.DataFrame(new_data, columns=target_fields[:-1])
    # Check for unique entries in new data and insert them
    unique_entries = df_new_data[~df_new_data.isin(df_existing_data)].dropna()
    if not unique_entries.empty:
        unique_entries['update_ts'] = dt
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.wp_detail',
                                                                             unique_entries.values,
                                                                             target_fields=target_fields)



with DAG(dag_id='get_wp_detail', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 1 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_wp_detail',
    python_callable=get_wp_detail
    )

t1
