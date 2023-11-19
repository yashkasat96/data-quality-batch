from pyspark.sql.types import StructField, StringType, IntegerType, TimestampType, StructType

JSON_EXTENSION = '.json'
PROPERTIES_EXTENSION = '.properties'
EQUAL_TO = '='
FORWARD_SLASH = '/'
GOOGLE_STORAGE_PATH = 'gs://'
BLANK = ""
DOT = '.'
COMMA = ','
PIPE = '|'

SAMPLE_SIZE = 5
ZERO = 0

ENTITY_TYPE = 'EntityType'
ENTITIES = 'entities'
KEY = 'Key'
COUNT = "count"
SOURCE = 'SOURCE'
TARGET = 'TARGET'
UniqueRowKey = 'UniqueRowKey'
ENTITY_NAME = 'EntityName'
VALUE = 'Value'
SUB_TYPE = 'SubType'
ENTITY_PHYSICAL_NAME = 'EntityPhysicalName'
PROPERTIES = 'Properties'
CSV = 'Csv'
PARQUET = 'Parquet'
BIG_QUERY = 'BigQuery'
HIVE = 'Hive'
CONSOLE = 'Console'
PROJECT_ID = 'project_id'
TEMP_GCS_BUCKET_NAME = 'temporaryGcsBucket'
MATERIALIZATION_DATASET = 'materializationDataset'
BIG_QUERY_DATA_SET_NAME = 'bq_dataset'
HIVE_DATABASE_NAME = 'hive_database'
TABLE = 'table'
PROJECT = 'project'
BIGQUERY = 'bigquery'
SUMMARY_TABLE_NAME = 'summary_table_name'
DETAILS_TABLE_NAME = 'details_table_name'


COMMA = ','
QUERY = 'QUERY'
PATH = 'PATH'
TOTAL_RECORD_SOURCE = 'TOTAL_RECORD_SOURCE'
SOURCE_TO_SOURCE = 'SOURCE_TO_SOURCE'
SOURCE_TO_TARGET = 'SOURCE_TO_TARGET'
TOTAL_RECORD_TARGET = 'TOTAL_RECORD_TARGET'
TARGET_TO_TARGET = 'TARGET_TO_TARGET'
RECORDS_MATCH = 'RECORDS_MATCH'
RECORDS_MISMATCH = 'RECORDS_MISMATCH'
UNIQUE_ROW_KEY = 'UNIQUE_ROW_KEY'
CONTEXT = 'CONTEXT'
ADDITIONAL_IN_SOURCE = 'ADDITIONAL_IN_SOURCE'
ADDITIONAL_IN_TARGET = 'MISSING_IN_TARGET'
COMPARISON_DETAILS_KEY = 'COMPARISON_DETAILS_KEY'
COMPARISON_SUMMARY_KEY = 'COMPARISON_SUMMARY_KEY'
TIME_CREATED = 'TIME_CREATED'
MATCHED_EXTRA_IN_SOURCE = 'MATCHED_EXTRA_IN_SOURCE'
SOURCE_COUNT = 'SOURCE_COUNT'
TARGET_COUNT = 'TARGET_COUNT'
SOURCE_VALUE = 'SOURCE_VALUE'
TARGET_VALUE = 'TARGET_VALUE'
MATCHED_EXTRA_IN_TARGET = 'MATCHED_EXTRA_IN_TARGET'
DUPLICATE_IN_SOURCE = 'DUPLICATE_IN_SOURCE'
DUPLICATE_IN_TARGET = 'DUPLICATE_IN_TARGET'
COMPARISON_CATEGORY = 'COMPARISON_CATEGORY'
COLUMN_MISSING_IN_TARGET = 'COLUMN_MISSING_IN_TARGET'
COLUMN_MISSING_IN_SOURCE = 'COLUMN_MISSING_IN_SOURCE'
COLUMN_MATCHED_DATA_TYPE_MISMATCHED = 'COLUMN_MATCHED_DATA_TYPE_MISMATCHED'
COLUMN_MATCHED_DATA_TYPE_MATCHED = 'COLUMN_MATCHED_DATA_TYPE_MATCHED'
SOURCE_COLUMN_COUNT = 'SOURCE_COLUMN_COUNT'
TARGET_COLUMN_COUNT = 'TARGET_COLUMN_COUNT'
JOB_ID = 'JOB_ID'
RULE_ID = 'RULE_ID'
COMPARISON_COUNT = 'COMPARISON_COUNT'
COMPARISON_DIRECTION = 'COMPARISON_DIRECTION'
SAMPLE_RESULT = 'SAMPLE_RESULT'


def summary_schema():
    schema = StructType([
        StructField(COMPARISON_SUMMARY_KEY, StringType(), True),
        StructField(JOB_ID, StringType(), True),
        StructField(RULE_ID, StringType(), True),
        StructField(SOURCE, StringType(), True),
        StructField(TARGET, StringType(), True),
        StructField(UNIQUE_ROW_KEY, StringType(), True),
        StructField(COMPARISON_CATEGORY, StringType(), True),
        StructField(COMPARISON_COUNT, IntegerType(), True),
        StructField(COMPARISON_DIRECTION, StringType(), True),
        StructField(SAMPLE_RESULT, StringType(), True),
        StructField(TIME_CREATED, TimestampType(), True)
    ])
    return schema


def details_schema():
    schema = StructType([
        StructField(COMPARISON_DETAILS_KEY, StringType(), True),
        StructField(COMPARISON_SUMMARY_KEY, StringType(), True),
        StructField(UNIQUE_ROW_KEY, StringType(), True),
        StructField(CONTEXT, StringType(), True),
        StructField(SOURCE_COUNT, IntegerType(), True),
        StructField(TARGET_COUNT, IntegerType(), True),
        StructField(SOURCE_VALUE, StringType(), True),
        StructField(TARGET_VALUE, StringType(), True),
        StructField(TIME_CREATED, TimestampType(), True)
    ])
    return schema
