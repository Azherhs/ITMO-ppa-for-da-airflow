import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator


def editors():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        INSERT INTO dds.editors (id, username, first_name, last_name, email, isu_number)
        SELECT DISTINCT (json_array_elements(wp_list::json->'editors')::json->>'id')::integer as editor_id, 
            (json_array_elements(wp_list::json->'editors')::json->>'username') as username,
            (json_array_elements(wp_list::json->'editors')::json->>'first_name') as first_name,
            (json_array_elements(wp_list::json->'editors')::json->>'last_name') as last_name,
            (json_array_elements(wp_list::json->'editors')::json->>'email') as email,
            (json_array_elements(wp_list::json->'editors')::json->>'isu_number') as isu_number
        FROM stg.su_wp sw
        ON CONFLICT ON CONSTRAINT editors_uindex DO UPDATE 
        SET 
            username = EXCLUDED.username, 
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name, 
            email = EXCLUDED.email, 
            isu_number = EXCLUDED.isu_number;
        """)


def states():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        TRUNCATE dds.states RESTART IDENTITY CASCADE;
        """
    )
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        INSERT INTO dds.states (cop_state, state_name)
        WITH t AS (
            SELECT DISTINCT (json_array_elements(wp_in_academic_plan::json)->>'status') as cop_states FROM stg.work_programs wp
        )
        SELECT cop_states, 
            CASE 
                WHEN cop_states = 'AC' THEN 'одобрено' 
                WHEN cop_states = 'AR' THEN 'архив'
                WHEN cop_states = 'EX' THEN 'на экспертизе'
                WHEN cop_states = 'RE' THEN 'на доработке'
                ELSE 'в работе'
            END as state_name
        FROM t
        ON CONFLICT ON CONSTRAINT state_name_uindex DO UPDATE 
        SET 
            id = EXCLUDED.id, 
            cop_state = EXCLUDED.cop_state;
        """)


def units():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        INSERT INTO dds.units (id, unit_title, faculty_id)
        SELECT DISTINCT sw.fak_id, 
            sw.fak_title, 
            ud.faculty_id::integer
        FROM stg.su_wp sw 
        LEFT JOIN stg.up_description ud 
        ON sw.fak_title = ud.faculty_name 
        ON CONFLICT ON CONSTRAINT units_uindex DO UPDATE 
        SET 
            unit_title = EXCLUDED.unit_title, 
            faculty_id = EXCLUDED.faculty_id;
        """)


def up():
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        TRUNCATE dds.up RESTART IDENTITY CASCADE;
        """
    )
    PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION").run(
        """
        INSERT INTO dds.up (app_isu_id, on_check, laboriousness, year, qualification, update_ts)
        SELECT ap_isu_id, on_check, laboriousness, 
            (json_array_elements(academic_plan_in_field_of_study::json)->>'year')::integer as year,
            (json_array_elements(academic_plan_in_field_of_study::json)->>'qualification') as qualification,
            NOW() as update_ts
        FROM stg.up_detail
        """
    )


def wp():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        TRUNCATE dds.wp RESTART IDENTITY CASCADE;
        """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        INSERT INTO dds.wp (wp_id, discipline_code, wp_title, wp_status, unit_id, wp_description, update_ts)
        WITH wp_desc AS (
            SELECT DISTINCT 
                json_array_elements(wp_in_academic_plan::json)->>'id' as wp_id,
                json_array_elements(wp_in_academic_plan::json)->>'discipline_code' as discipline_code,
                json_array_elements(wp_in_academic_plan::json)->>'description' as wp_description,
                json_array_elements(wp_in_academic_plan::json)->>'status' as wp_status,
                update_ts
            FROM stg.work_programs wp
        ),
        wp_unit AS (
            SELECT 
                fak_id,
                wp_list::json->>'id' as wp_id,
                wp_list::json->>'title' as wp_title,
                (wp_list::json->>'discipline_code') as discipline_code
            FROM stg.su_wp sw
        )
        SELECT DISTINCT 
            wp_desc.wp_id::integer, 
            wp_desc.discipline_code,
            wp_unit.wp_title,
            s.id as wp_status, 
            wp_unit.fak_id as unit_id,
            wp_desc.wp_description,
            NOW() as update_ts
        FROM wp_desc
        LEFT JOIN wp_unit
        ON wp_desc.discipline_code = wp_unit.discipline_code
        LEFT JOIN dds.states s 
        ON wp_desc.wp_status = s.cop_state;
        """)


def wp_inter():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        TRUNCATE dds.wp_editor RESTART IDENTITY CASCADE;
        """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        TRUNCATE dds.wp_up RESTART IDENTITY CASCADE;
        """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        INSERT INTO dds.wp_editor (wp_id, editor_id)
        SELECT (wp_list::json->>'id')::integer as wp_id,
            (json_array_elements(wp_list::json->'editors')::json->>'id')::integer as editor_id
        FROM stg.su_wp
        """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
        """
        INSERT INTO dds.wp_up (wp_id, up_id)
        WITH t AS (
            SELECT id,
                (json_array_elements(wp_in_academic_plan::json)->>'id')::integer as wp_id,
                json_array_elements(academic_plan_in_field_of_study::json)->>'ap_isu_id' as up_id
            FROM stg.work_programs wp
        )
        SELECT t.wp_id, (json_array_elements(wp.academic_plan_in_field_of_study::json)->>'ap_isu_id')::integer as up_id 
        FROM t
        JOIN stg.work_programs wp
        ON t.id = wp.id
        WHERE wp_id IS NOT NULL AND up_id is NOT NULL
        """)


with DAG(dag_id='stg_to_dds', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 4 * * *', catchup=False) as dag:
    t1 = PythonOperator(
        task_id='editors',
        python_callable=editors
    )

    t2 = PythonOperator(
        task_id='states',
        python_callable=states
    )

    t3 = PythonOperator(
        task_id='units',
        python_callable=units
    )

    t4 = PythonOperator(
        task_id='up',
        python_callable=up
    )

    t5 = PythonOperator(
        task_id='wp',
        python_callable=wp
    )

    t6 = PythonOperator(
        task_id='wp_inter',
        python_callable=wp_inter
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
