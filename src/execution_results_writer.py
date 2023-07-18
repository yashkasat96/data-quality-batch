from pyspark.sql.types import StructField, StringType, TimestampType, IntegerType, StructType


def run_stats_schema():
    schema = StructType([
        StructField('job_run_id', StringType(), True),
        StructField('ruleset_id', StringType(), True),
        StructField('start_time', TimestampType(), True),
        StructField('end_time', TimestampType(), True),
        StructField('created_time', TimestampType(), True)
    ])
    return schema


def rule_run_stats_schema():
    schema = StructType([
        StructField('job_run_id', StringType(), True),
        StructField('ruleset_id', IntegerType(), True),
        StructField('rule_id', IntegerType(), True),
        StructField('total_records', IntegerType(), True),
        StructField('pass_count', IntegerType(), True),
        StructField('fail_count', IntegerType(), True),
        StructField('start_time', TimestampType(), True),
        StructField('end_time', TimestampType(), True),
        StructField('created_time', TimestampType(), True)
    ])
    return schema


def query_stats_schema():
    schema = StructType([
        StructField('job_run_id', StringType(), True),
        StructField('rule_id', IntegerType(), True),
        StructField('query', IntegerType(), True),
        StructField('start_time', TimestampType(), True),
        StructField('end_time', TimestampType(), True),
        StructField('total_execution_time', IntegerType(), True),
        StructField('created_time', TimestampType(), True)
        ])
    return schema


def rule_exceptions():
    schema = StructType([
        StructField('rule_id', IntegerType(), True),
        StructField('rule_set_id', IntegerType(), True),
        StructField('data_object_key', StringType(), True),
        StructField('exception_summary', StringType(), True),
        StructField('created_time', TimestampType(), True)
        ])
    return schema


class ExecutionResultsWriter:
    def __init__(self, context):
        self.context = context

    def write(self,result):
        pass

