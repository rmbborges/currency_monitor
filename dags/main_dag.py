from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import datetime

from helpers.queries import SqlQueries

default_args = {
    "owner": "ricardo",
    "start_date": datetime.datetime.now(),
    "depends_on_past": False,
    "retries": 0,
    "catchup": False,
    "email_on_retry": False
}

dag = DAG(
    "main_dag",
    default_args=default_args,
    description="Consulta os valores agora e decide se precisa fazer um tweet ou não.",
    max_active_runs=1,
    schedule_interval="*/5 9-16 * * 1-5"
)

start = DummyOperator(dag=dag, task_id="start")

create_currency_schema = PostgresOperator(
    dag=dag,
    task_id="create_currency_schema",
    sql=SqlQueries.CREATE_CURRENCY_SCHEMA,
    autocommit=True,
    postgres_conn_id="production_postgres"
)

create_data_table = PostgresOperator(
    dag=dag,
    task_id="create_data_table",
    sql=SqlQueries.CREATE_DATA_TABLE,
    autocommit=True,
    postgres_conn_id="production_postgres"
)

end = DummyOperator(dag=dag, task_id="end")

start >> create_currency_schema >> create_data_table >> end