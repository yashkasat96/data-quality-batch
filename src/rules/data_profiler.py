import os

from pydeequ import ColumnProfilerRunner
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.query_executor import execute_rule_queries
from src.reader import read
from src.utils import get_current_time, get_spark_session, get_unique_id
import pydeequ
from pydeequ.analyzers import *


class Profiler:
    def __init__(self, context):
        self.source = None
        self.context = context
        self.rule = self.context.get_current_rule()
        self.results = {}

    def execute(self):
        source_query = self.context.get_rule_property('SOURCE_QUERY')
        source_entity = self.context.get_source_entity()
        self.results['source_query'] = source_query
        source_query_execution_start_time = get_current_time()
        self.source = read(source_entity, source_query, self.context)
        self.results['source_query_end_time'] = get_current_time()
        self.results['source_query_start_time'] = source_query_execution_start_time

        result = ColumnProfilerRunner(get_spark_session()) \
            .onData(self.source) \
            .run()
        columns = []
        column_profiles = []

        profile_summary_key = get_unique_id()
        for col, profile in result.profiles.items():
            columns.append(col)
            column_profile = json.loads(str(profile).split(':', 2)[2])
            column_profile['column_name'] = col
            column_profiles.append(column_profile)
            print(column_profile)
            profile_details_key = get_unique_id()



        entity = self.context.get_source_entity()
        primary_key = entity['primary_key']
        entity_sub_type = entity['entity_sub_type']
        entity_physical_name = entity['entity_physical_name']
        if entity_sub_type == 'BIG_QUERY':
            entity_physical_name = entity['entity_name']
        base_criteria = base_criteria.replace('{BASE_CRITERIA_COLUMN}', base_criteria_column)
        failed_records_query = f"select {primary_key} from {entity_physical_name} where {base_criteria} and {filter_condition}"
        total_records_query = f"select count(*) as total_count from {entity_physical_name} where {filter_condition}"
        return execute_rule_queries(entity, failed_records_query, total_records_query, self.context)

    def details_schema(self):
        schema = StructType([
            StructField("PROFILER_DETAILS_KEY", StringType(), True),
            StructField("PROFILER_SUMMARY_KEY", StringType(), True),
            StructField("ATTRIBUTE_NAME", StringType(), True),
            StructField("NULL_PERCENTAGE", IntegerType(), True),
            StructField("UNIQUE_PERCENTAGE", IntegerType(), True),
            StructField("DISTINCT_VALUES_COUNT", IntegerType(), True),
            StructField("EXISTING_DATA_TYPE", StringType(), True),
            StructField("IDENTIFIED_DATA_TYPE", StringType(), True),
            StructField("MIN_VALUE", IntegerType(), True),
            StructField("MAX_VALUE", IntegerType(), True),
            StructField("AVERAGE_VALUE", IntegerType(), True),
            StructField("MIN_LENGTH", IntegerType(), True),
            StructField("MAX_LENGTH", IntegerType(), True),
            StructField("AVERAGE_LENGTH", IntegerType(), True),
            StructField("STD_DEVIATION", IntegerType(), True),
            StructField("MODE", IntegerType(), True),
            StructField("MEDIAN", IntegerType(), True),
            StructField("OUTLIER_PER", IntegerType(), True),
            StructField("TIME_CREATED", TimestampType(), True)
        ])
        return schema
