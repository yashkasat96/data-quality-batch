import math
import operator
import os

os.environ["SPARK_VERSION"] = "3.2"

from pydeequ import ColumnProfilerRunner
from pydeequ.analyzers import *
from pyspark.pandas.spark.functions import lit
from pyspark.sql import Window
from pyspark.sql.functions import desc, length, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType, DoubleType

from src.reader import read
from src.utils import get_current_time, get_spark_session, get_unique_id, get_empty_data_frame


class Profiler:
    def __init__(self, context):

        self.source = None
        self.context = context
        self.rule = self.context.get_current_rule()
        self.results = {}
        self.profile_details = get_empty_data_frame(self.profile_details_schema())
        self.profile_summary = get_empty_data_frame(self.profile_summary_schema())
        self.profile_column_details = get_empty_data_frame(self.profile_column_details_schema())
        self.time_created = get_current_time()

    def execute(self):
        os.environ["SPARK_VERSION"] = "3.2"
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

        total_count = self.source.count()

        source_count_df = self.source.groupBy(*[column_name for column_name in self.source.columns]).count()
        unique_records_count_in_source = source_count_df.filter("count == 1").count()

        self.results['source_count'] = total_count
        profile_summary_key = get_unique_id()
        column_count = len(result.profiles.items())

        for column_name, profile in result.profiles.items():
            profile_details_key = get_unique_id()
            min_value = max_value = value_range = average_value = mode = median = std_deviation = outlier_per = 'NA'
            column_profile = json.loads(str(profile).split(':', 2)[2])
            column_profile['column_name'] = column_name
            null_percentage = 1 - column_profile['completeness']
            unique_percentage = (column_profile['approximateNumDistinctValues'] / total_count) * 100
            existing_data_type = column_profile['dataType']
            identified_data_type = column_profile['dataType']

            type_counts = column_profile['typeCounts']
            if type_counts and type_counts[identified_data_type] != total_count:
                percent_type_counts = {key: (value / total_count) * 100 for key, value in type_counts.items() if
                                       value != 0}
                profile_column_details_rows = []
                for data_type, percent in percent_type_counts.items():
                    row = [get_unique_id(), profile_details_key, 'IDENTIFIED_DATA_TYPES', data_type, percent,
                           get_current_time()]
                    profile_column_details_rows.append(row)

                identified_data_type = max(percent_type_counts.items(), key=operator.itemgetter(1))[0]

                self.profile_column_details = self.profile_column_details.union(
                    get_spark_session().createDataFrame(profile_column_details_rows,
                                                        self.profile_column_details_schema()))

            if column_profile['dataType'] == 'Integral' or column_profile['dataType'] == 'Fractional':
                min_value = column_profile['minimum']
                max_value = column_profile['maximum']
                value_range = max_value - min_value
                average_value = self.round_up(column_profile['mean'])
                mode = self.calculate_mode(self.source.select(column_name), column_name)
                #median = self.calculate_median(self.source.select(column_name), column_name,
                #                            total_count)
                std_deviation = self.round_up(column_profile['stdDev'])
                outlier_per = self.round_up(self.calculate_outliers(self.source, column_name, total_count))

            average_length, min_length, max_length = self.calculate_length(self.source.select(column_name), column_name)

            row = [profile_details_key, profile_summary_key, column_name, self.round_up(null_percentage),
                   self.round_up(unique_percentage), column_profile['approximateNumDistinctValues'],
                   existing_data_type,
                   identified_data_type, min_value, max_value, value_range, average_value, mode, std_deviation,
                   outlier_per, min_length,
                   max_length, self.round_up(average_length), self.time_created
                   ]
            self.profile_details = self.profile_details.union(
                get_spark_session().createDataFrame([row], self.profile_details_schema()))

        profile_summary_row = [profile_summary_key, self.context.get_job_run_id(), self.context.get_rule_id(),
                               source_entity['entity_name'], total_count, column_count,
                               self.round_up((unique_records_count_in_source / total_count) * 100), get_current_time()]

        self.profile_summary = self.profile_summary.union(
            get_spark_session().createDataFrame([profile_summary_row], self.profile_summary_schema()))

        self.results['profiler_details'] = self.profile_details
        self.results['profiler_summary'] = self.profile_summary

        if self.profile_column_details.count() > 0:
            self.results['profiler_column_details'] = self.profile_column_details

        self.results['is_profiler'] = True

        return self.results

    def calculate_median(self, df, column_name, total_count):
        middle_index = int(total_count / 2)
        window_spec = Window.orderBy(df[column_name])
        df = df.withColumn("row_num", row_number().over(window_spec))
        df = df.filter((df["row_num"] == lit(middle_index)) | (df["row_num"] == lit(middle_index + 1)))
        middle_index_val = df.filter((df["row_num"] == lit(middle_index))).select(column_name).collect()[0][column_name]
        middle_index_plus_1_val = df.filter((df["row_num"] == lit(middle_index + 1))).select(column_name).collect()[0][
            column_name]

        if total_count % 2 == 0:
            med = middle_index_val
        else:
            med = (middle_index_val + middle_index_plus_1_val) / 2

        return med

    def calculate_length(self, df, column_name):
        df1 = df.withColumn('converted' + column_name, df[column_name].cast("string"))
        df1 = df1.withColumn('length', length(df1['converted' + column_name]))
        avg = df1.agg({"length": "avg"}).collect()[0]['avg(length)']
        min = df1.agg({"length": "min"}).collect()[0]['min(length)']
        max = df1.agg({"length": "max"}).collect()[0]['max(length)']

        return avg, min, max

    def calculate_outliers(self, df, column_name, total_count):
        quartiles = df.approxQuantile(column_name, [0.25, 0.75], 0.05)
        q1, q3 = quartiles
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers_count = df.filter((df[column_name] < lower_bound) | (df[column_name] > upper_bound)).count()
        return (outliers_count / total_count) * 100

    def calculate_mode(self, df, column_name):
        grouped_df = df.groupBy(column_name).count()
        sorted_df = grouped_df.orderBy(desc('count'))
        return sorted_df.first()[column_name]

    def profile_details_schema(self):
        schema = StructType([
            StructField("PROFILER_DETAILS_KEY", StringType(), True),
            StructField("PROFILER_SUMMARY_KEY", StringType(), True),
            StructField("ATTRIBUTE_NAME", StringType(), True),
            StructField("NULL_PERCENTAGE", FloatType(), True),
            StructField("UNIQUE_PERCENTAGE", FloatType(), True),
            StructField("DISTINCT_VALUES_COUNT", IntegerType(), True),
            StructField("EXISTING_DATA_TYPE", StringType(), True),
            StructField("IDENTIFIED_DATA_TYPE", StringType(), True),
            StructField("MIN_VALUE", StringType(), True),
            StructField("MAX_VALUE", StringType(), True),
            StructField("RANGE", StringType(), True),
            StructField("AVERAGE_VALUE", StringType(), True),
            StructField("MODE", StringType(), True),
            StructField("STD_DEVIATION", StringType(), True),
            StructField("OUTLIER_PER", StringType(), True),
            StructField("MIN_LENGTH", IntegerType(), True),
            StructField("MAX_LENGTH", IntegerType(), True),
            StructField("AVERAGE_LENGTH", FloatType(), True),
            StructField("TIME_CREATED", TimestampType(), True)
        ])
        return schema

    def profile_summary_schema(self):
        schema = StructType([
            StructField("PROFILER_SUMMARY_KEY", StringType(), True),
            StructField("JOB_ID", StringType(), True),
            StructField("RULE_ID", StringType(), True),
            StructField("SOURCE", StringType(), True),
            StructField("ROW_COUNT", IntegerType(), True),
            StructField("COLUMN_COUNT", IntegerType(), True),
            StructField("UNIQUE_PERCENTAGE", FloatType(), True),
            StructField("TIME_CREATED", TimestampType(), True)
        ])
        return schema

    def profile_column_details_schema(self):
        schema = StructType([
            StructField("PROFILER_COLUMN_DETAILS_KEY", StringType(), True),
            StructField("PROFILER_DETAILS_KEY", StringType(), True),
            StructField("DETAILS_TYPE", StringType(), True),
            StructField("DATA_TYPE", StringType(), True),
            StructField("PERCENT", FloatType(), True),
            StructField("TIME_CREATED", TimestampType(), True)
        ])
        return schema

    def round_up(self, value):
        return math.ceil(value * 100) / 100
