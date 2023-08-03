import airflow
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import timedelta

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dq_executor_dag',
    default_args=default_args,
    description='DAG to schedule jobs for falcon data platform',
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
)

DATA_QUALITY_PYSPARK_JOB = {
    "reference": {"project_id": 'playground-375318'},
    "placement": {"cluster_name": 'cluster-c6fd'},
    "pyspark_job": {"main_python_file_uri": 'gs://falcon-data-platform/operations/data_quality/src/main.py',
                    "args": ['app_conf=gs://falcon-data-platform/operations/data_quality/conf/application_bq.properties,'
                             'rule_set_path=gs://falcon-data-platform/operations/data_quality/rule/account_bank_ruleset_bq.json,'
                             'job_id=12345685'],
                    "jar_file_uris": ['gs://falcon-data-platform/operations/data_quality/lib/spark-bigquery-with-dependencies_2.12-0.31.1.jar'],
                    "python_file_uris": ['gs://falcon-data-platform/operations/data_quality/src/app_context.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/constants.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/execution_results_writer.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/executor.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/reader.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/rule_set_executor.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/utils.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/writer.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/query_executor.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/data_comparator.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/null_check.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/length_check.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/reference_values_check.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/uniqueness_check.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/range_check.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/rule_factory.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/sql_validator.py',
                                         'gs://falcon-data-platform/operations/data_quality/src/__init__.py']

                    },
}


data_quality_pyspark_task = DataprocSubmitJobOperator(
    task_id="data_quality",
    job=DATA_QUALITY_PYSPARK_JOB,
    region='us-central1',
    project_id='playground-375318',
    dag=dag,

)

