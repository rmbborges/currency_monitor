from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import datetime

from helpers.queries import SqlQueries
from operators.loaders import LoadClosingReferenceData

currencies_list = Variable.get("currencies")

default_args = {
    "owner": "ricardo",
    "start_date": datetime.datetime.now(),
    "depends_on_past": False,
    "retries": 0,
    "catchup": False,
    "email_on_retry": False
}

dag = DAG(
    "reference_data_dag",
    default_args=default_args,
    description="Consulta os dados históricos das moedas",
    max_active_runs=1,
    schedule_interval="0 9 * * 1-5"
)

start = DummyOperator(dag=dag, task_id="start")

create_currency_schema = PostgresOperator(
    dag=dag,
    task_id="create_currency_schema",
    sql=SqlQueries.CREATE_CURRENCY_SCHEMA,
    autocommit=True,
    postgres_conn_id="production_postgres"
)

create_closing_reference_table = PostgresOperator(
    dag=dag,
    task_id="create_closing_reference_table",
    sql=SqlQueries.CREATE_CLOSING_REFERENCE_TABLE,
    autocommit=True,
    postgres_conn_id="production_postgres"
)

load_closing_reference_data = LoadClosingReferenceData(
    dag=dag,
    task_id="load_closing_reference_data",
    postgres_conn_id="production_postgres"
)

end = DummyOperator(dag=dag, task_id="end")

start >> create_currency_schema >> create_closing_reference_table >> load_closing_reference_data >> end