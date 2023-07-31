from airflow import DAG
from airflow.operators.dataproc_operator import DataprocSubmitPySparkJobOperator
from airflow.operators.dataproc_operator import DataprocSubmitJobOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2023, 7, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('gcp_cloud_composer', default_args=default_args, schedule_interval=None)

jobs = ['data_generator', 'restonomer', 'minimal', 'data_quality', 'data_dictionary']
python_jobs = ['data_generator', 'data_dictionary']
spark_jobs = ['minimal', 'restonomer', 'data_quality']

# Submit Python jobs
for job in python_jobs:
    submit_python_job = DataprocSubmitPySparkJobOperator(
        task_id=f'submit_{job}_job',
        job_name=f'{job}_job',
        main='gs://your-bucket/your-python-job.py',
        cluster_name='your-cluster',
        dag=dag
    )

    if job == 'data_generator':
        submit_python_job.set_upstream(None)
    else:
        submit_python_job.set_upstream(submit_python_job_prev)

    submit_python_job_prev = submit_python_job

# Submit Spark jobs
submit_spark_jobs = []
for job in spark_jobs:
    submit_spark_job = DataprocSubmitJobOperator(
        task_id=f'submit_{job}_job',
        job_name=f'{job}_job',
        region='your-region',
        cluster_name='your-cluster',
        job={
            'reference': {
                'projectId': 'your-project-id',
                'jobId': f'{job}_job'
            },
            'placement': {
                'clusterName': 'your-cluster'
            },
            'pysparkJob': {
                'mainPythonFileUri': 'gs://your-bucket/your-spark-job.py'
            }
        },
        dag=dag
    )

    if job == 'minimal':
        submit_spark_job.set_upstream(submit_python_job_prev)
    else:
        submit_spark_job.set_upstream(submit_spark_job_prev)

    submit_spark_jobs.append(submit_spark_job)
    submit_spark_job_prev = submit_spark_job

# Set parallelism for the last two Spark jobs
submit_spark_jobs[-1].set_downstream(submit_spark_jobs[-2])