from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

import sys
sys.path.append("/app") 

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=1),
    "email_on_retry": False,
    "retries": 1,
}

from src.DataPipline import *
from src.DataIntegration import *
from src.Utils import *
from src.DataEncoding import *
from src.PrepareStream import *
from src.KafkaConsumer import * 
from src.KafkaProducer import *
from src.SparkProcessing import *
from src.DataVisualisation import *

with DAG(
    dag_id="stock_portfolio_pipeline_<teamname>",
    default_args=default_args,
    description="End-to-end stock portfolio analytics pipeline",
    schedule_interval="@daily",
    catchup=False,
    tags=["data-engineering", "stocks", "analytics"],
) as dag:
    with TaskGroup("stage_1_data_cleaning") as stage_1:
        t1 = PythonOperator(
            task_id="clean_missing_values",
            python_callable=impute_missing_data,
        )

        t2 = PythonOperator(
            task_id="detect_outliers",
            python_callable=detect_and_handle_outliers,
        )

        t3 = PythonOperator(
            task_id="integrate_datasets",
            python_callable=integrate_data,
        )

        t4= PythonOperator(
            task_id="load_to_postgres",
            python_callable=save_to_db,
        )
    t1>>t2>>t3>>t4

    with TaskGroup("stage_2_data_encoding") as stage_2:
        t5 = PythonOperator(
            task_id="prepare_streaming_data",
            python_callable=prepare_stream,
        )

        t6 = PythonOperator(
            task_id="encode_categorical_data",
            python_callable=encode_data,
        )
    t5 >> t6

    with TaskGroup("stage_3_stream_processing") as stage_3:
        t7 = PythonOperator(
            task_id="start_kafka_producer",
            python_callable=kafka_producer,
        )

        t8 = PythonOperator(
            task_id="consume_and_process_stream",
            python_callable=consumer_stream,
        )

        t9 = PythonOperator(
            task_id="save_final_to_postgres",
            python_callable=save_to_db,
        )
    t7 >> t8 >> t9

    with TaskGroup("stage_4_spark_processing") as stage_4:
        t10 = PythonOperator(
            task_id="initialize_spark_session",
            python_callable=initialize_spark_session,
        )
        t11 = PythonOperator(
            task_id="run_spark_analytics",
            python_callable=run_spark_analytics,
        )
    t10 >> t11

    with TaskGroup("stage_5_data_visualization") as stage_5:
        t12 = PythonOperator(
            task_id="prepare_visualization_data",
            python_callable=prepare_visualization,
        )
        t13 = BashOperator(
            task_id="start_visualization_service",
            bash_command="streamlit /app/streamlit/app.py",
        )
    t12 >> t13
    