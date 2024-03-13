import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

url = "https://op.itmo.ru/auth/token/login"
username = Variable.get("username")
password = Variable.get("password")
auth_data = {"username": username, "password": password}

token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["auth_token"]
headers = {"Content-Type": "application/json", "Authorization": "Token " + token}

#id  >= 7220
def get_up_detail():

    ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    select id as op_id
    from stg.work_programs wp
    where id > 7220
    order by 1
    """)
    existing_data = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").get_records(
    """
    SELECT id, ap_isu_id, on_check, laboriousness, academic_plan_in_field_of_study, update_ts
        FROM stg.up_detail
    """)

    url_down = 'https://op.itmo.ru/api/academicplan/detail/'
    target_fields = ['id', 'ap_isu_id', 'on_check', 'laboriousness', 'academic_plan_in_field_of_study', 'update_ts']
    df_existing_data = pd.DataFrame(existing_data, columns=target_fields)
    for op_id in ids:
        op_id = str(op_id[0])
        print(op_id)
        dt = pendulum.now("Europe/Moscow").to_iso8601_string()
        url = url_down + op_id + '?format=json'
        page = requests.get(url, headers=headers)
        df = pd.DataFrame.from_dict(page.json(), orient='index')
        df = df.T
        df['academic_plan_in_field_of_study'] = df[~df['academic_plan_in_field_of_study'].isna()]["academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
        df.loc[:, "update_ts"] = dt
        df = df[target_fields]
        overlapping_entries = df_existing_data[
            (df_existing_data["id"] == df["id"].iloc[0])]
        if overlapping_entries.empty:
            # Insert the new entry with an updated update_ts
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.up_detail',
                                                                                 df.values,
                                                                                 target_fields = target_fields)
            



with DAG(
    dag_id="get_up_detail",
    start_date=pendulum.datetime(2023, 1, 10, tz="UTC"),
    schedule_interval="0 1 * * *",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="get_up_detail", python_callable=get_up_detail)

t1