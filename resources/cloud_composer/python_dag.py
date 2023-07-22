"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import timedelta
from external.test1 import test1_placeholder

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'falcon-dag',
    default_args=default_args,
    description='DAG to schedule jobs for falcon data platform',
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

data_generation_python_task = PythonOperator(
    task_id='data_generation_python_task',
    python_callable=test1_placeholder,
    dag=dag
)

PYSPARK_JOB = {
    "reference": {"project_id": 'playground-375318'},
    "placement": {"cluster_name": 'cluster-c6fd'},
    "pyspark_job": {"main_python_file_uri": 'gs://data-comparator-demo/data_quality_batch/src/main.py',
                    "args": ['app_conf=gs://data-comparator-demo/rule/application_bq.properties,'
                             'rule_set_path=gs://data-comparator-demo/rule/employee_payments_rule.json,'
                             'job_id=12345680'],
                    "jar_file_uris": ['gs://data-comparator-demo/lib/spark-bigquery-with-dependencies_2.12-0.31.1.jar'],
                    "python_file_uris": ['gs://data-comparator-demo/data_quality_batch/src/app_context.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/constants.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/execution_results_writer.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/executor.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/reader.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/rule_set_executor.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/utils.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/writer.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/query_executor.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/data_comparator.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/natural_lang_rule.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/null_check.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/range_check.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/rule_factory.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/sql_validator.py',
                                         'gs://data-comparator-demo/data_quality_batch/src/__init__.py']

                    },
}

data_quality_pyspark_task = DataprocSubmitJobOperator(
    task_id="data_quality_pyspark_task",
    job=PYSPARK_JOB,
    region='us-central1',
    project_id='playground-375318',
    dag=dag,
)
