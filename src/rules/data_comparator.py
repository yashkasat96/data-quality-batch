from src.reader import read
from src.utils import get_spark_session, get_empty_data_frame, get_unique_id
from datetime import datetime
from src.constants import *


class DataComparator:
    def __init__(self, context):
        self.context = context
        self.rule = None
        self.job_id = None
        self.time_created = datetime.now()
        self.source_unique_key = None
        self.source_unique_key_array = None
        self.target_unique_key = None
        self.target_unique_key_array = None
        self.sourceEntityName = None
        self.targetEntityName = None
        self.summary = get_empty_data_frame(summary_schema())
        self.details = get_empty_data_frame(details_schema())
        self.source_entity_name = None
        self.target_entity_name = None

    def execute(self, rule):
        self.rule = rule
        source_entity = self.context.get_source_entity(self.rule)
        target_entity = self.context.get_target_entity(self.rule)
        self.source_unique_key = self.context.get_unique_row_key(source_entity)
        self.target_unique_key_array = self.source_unique_key.split(',')
        self.target_unique_key_array = self.target_unique_key.split(',')
        self.target_unique_key = self.context.get_unique_row_key(target_entity)
        self.source_entity_name = self.context.get_entity_name(source_entity)
        self.target_entity_name = self.context.get_entity_name(target_entity)
        self.job_id = self.context.get_job_id()
        source_query = self.context.get_property('source_query', self.rule)
        target_query = self.context.get_property('target_query', self.rule)
        source = read(source_entity, source_query)
        target = read(target_entity, target_query)
        summary, details = self.compare(source, target)

    def compare(self, source, target):
        self.compare_counts(source, target)

        source_deduplicated = source.dropDuplicates()
        target_deduplicated = target.dropDuplicates()

        self.compare_matching(source_deduplicated, target_deduplicated)
        self.compare_missing(source_deduplicated, target_deduplicated)

        source_count_df = source.groupBy(*self.source_unique_key_array).count()
        target_count_df = target.groupBy(*self.target_unique_key_array).count()

        self.compare_duplicates(source_count_df, target_count_df)
        self.compare_extra(source_count_df, target_count_df)

        return self.summary, self.details

    def compare_counts(self, source, target):
        source_total_record_list = [get_unique_id(), self.job_id, self.sourceEntityName, self.targetEntityName
            , self.source_unique_key, TOTAL_RECORD_SOURCE, source.count(),
                                    SOURCE_TO_SOURCE, BLANK, self.time_created]

        self.summary = self.summary.union(
            get_spark_session().createDataFrame([source_total_record_list], summary_schema()))

        target_total_record_list = [getUniqueId(), self.job_id, self.sourceEntityName, self.targetEntityName,
                                    self.source_unique_key, TOTAL_RECORD_TARGET, target.count(),
                                    TARGET_TO_TARGET, BLANK, self.time_created]

        self.summary = self.summary.union(
            getSparkSession().createDataFrame([target_total_record_list], summary_schema()))

    def compare_matching(self, source_deduplicated, target_deduplicated):
        # computing record match and mismatch
        join_columns = [source_deduplicated[column_name] for column_name in target_deduplicated.columns if
                        column_name in self.source_unique_key_array]
        join_conditions = [source_deduplicated[column_name] == target_deduplicated[column_name] for column_name in
                           self.source_unique_key_array]

        conditions_ = [when(source_deduplicated[column_name] != target_deduplicated[column_name], column_name)
                       .otherwise(BLANK)
                       for column_name in source_deduplicated.columns
                       if column_name not in self.source_unique_key_array]

        select_expr = [*join_columns, array_remove(array(*conditions_), BLANK).alias("column_names")]
        merged = source_deduplicated.join(target_deduplicated, join_conditions).select(*select_expr)
        records_match = merged.filter("column_names = array()")
        comparison_summary_key = getUniqueId()
        self.build_summary(records_match, self.get_sample_record(records_match), RECORDS_MATCH, comparison_summary_key)
        # records mismatch summary
        records_mis_match = merged.filter("column_names != array()")
        comparison_summary_key = getUniqueId()
        self.build_summary(records_mis_match, self.get_sample_record(records_mis_match), RECORDS_MISMATCH,
                           comparison_summary_key)

        record_mismatch_with_column = records_mis_match.select(*join_columns,
                                                               explode(records_mis_match.column_names)).dropDuplicates()

        record_mismatch_details = self.build_details(record_mismatch_with_column, comparison_summary_key) \
            .drop(col(CONTEXT)) \
            .withColumnRenamed("col", CONTEXT)

        self.union_details(record_mismatch_details)

    def compare_missing(self, source_deduplicated, target_deduplicated):
        comparison_summary_key = getUniqueId()
        join_conditions = [source_deduplicated[column_name] == target_deduplicated[column_name]
                           for column_name in self.source_unique_key_array]

        target_join_columns = [target_deduplicated[column_name] for column_name in target_deduplicated.columns if
                               column_name in self.source_unique_key_array]

        source_join_columns = [source_deduplicated[column_name] for column_name in source_deduplicated.columns if
                               column_name in self.source_unique_key_array]
        target_deduplicated = target_deduplicated.select(*target_join_columns).dropDuplicates()
        source_deduplicated = source_deduplicated.select(*source_join_columns).dropDuplicates()
        missing_in_source = target_deduplicated.join(source_deduplicated,
                                                     join_conditions,
                                                     "leftanti"
                                                     )
        missing_in_source_sample = missing_in_source.withColumn('almost_desired_output', concat_ws('|',
                                                                                                   *self.source_unique_key_array)).dropDuplicates().take(
            SAMPLE_SIZE)

        missing_in_source_sample_array = []
        for row in missing_in_source_sample:
            missing_in_source_sample_array.append(str(row[len(self.source_unique_key_array)]))

        self.build_summary(missing_in_source, missing_in_source_sample_array,
                           MISSING_IN_SOURCE,
                           comparison_summary_key)

        # Missing in Source details
        self.union_details(self.build_details(missing_in_source, comparison_summary_key))

        # Missing in Target Summary
        comparison_summary_key = getUniqueId()
        missing_in_target = source_deduplicated.join(target_deduplicated,
                                                     join_conditions,
                                                     "leftanti")

        missing_in_target_sample = missing_in_target.withColumn('almost_desired_output', concat_ws('|',
                                                                                                   *self.source_unique_key_array)).dropDuplicates().take(
            SAMPLE_SIZE)

        missing_in_source_sample_array = []
        for row in missing_in_target_sample:
            missing_in_source_sample_array.append(str(row[len(self.source_unique_key_array)]))

        self.build_summary(missing_in_target, missing_in_source_sample_array, MISSING_IN_TARGET, comparison_summary_key)
        # Missing in Target Details
        self.union_details(self.build_details(missing_in_target, comparison_summary_key))

    def compare_duplicates(self, source_count_df, target_count_df):
        comparison_summary_key = get_unique_id()
        duplicate_in_source = source_count_df.filter("count > 1")

        self.build_summary(duplicate_in_source, self.get_sample_record(duplicate_in_source), DUPLICATE_IN_SOURCE,
                           comparison_summary_key, SOURCE_TO_SOURCE)

        # Duplicate in Source details
        duplicate_in_source_details = self.build_details(duplicate_in_source, comparison_summary_key) \
            .drop(col(SOURCE_COUNT)) \
            .withColumnRenamed(COUNT, SOURCE_COUNT)

        self.union_details(duplicate_in_source_details)

        # Duplicate in Target Summary
        comparison_summary_key = getUniqueId()
        duplicate_in_target = target_count_df.filter("count > 1")

        self.build_summary(duplicate_in_target, self.get_sample_record(duplicate_in_target), DUPLICATE_IN_TARGET,
                           comparison_summary_key, TARGET_TO_TARGET)

        # Duplicate in Target Details
        duplicate_in_target_details = self.build_details(duplicate_in_target, comparison_summary_key) \
            .drop(col(TARGET_COUNT)) \
            .withColumnRenamed(COUNT, TARGET_COUNT)

        self.union_details(duplicate_in_target_details)

    def compare_extra(self, source_count_df, target_count_df):
        # Calculate Extra In Source and Target
        source_count = source_count_df.withColumnRenamed(COUNT, SOURCE_COUNT)
        target_count = target_count_df.withColumnRenamed(COUNT, TARGET_COUNT)

        join_conditions = [source_count[column_name] == target_count[column_name]
                           for column_name in self.source_unique_key_array]

        join_columns = [source_count[column_name] for column_name in source_count.columns if
                        column_name in self.source_unique_key_array]

        counts_joined = source_count.join(target_count, join_conditions).select(*join_columns,
                                                                                SOURCE_COUNT,
                                                                                TARGET_COUNT)
        extra_in_source = counts_joined.filter("SOURCE_COUNT > TARGET_COUNT")
        extra_in_target = counts_joined.filter("TARGET_COUNT > SOURCE_COUNT")

        # extra in source summary
        comparison_summary_key = getUniqueId()
        self.build_summary(extra_in_source, self.get_sample_record(extra_in_source), EXTRA_IN_SOURCE,
                           comparison_summary_key)

        # extra in source details
        self.union_details(self.build_details(extra_in_source, comparison_summary_key, False))

        # extra in target
        comparison_summary_key = getUniqueId()
        self.build_summary(extra_in_target, self.get_sample_record(extra_in_target), EXTRA_IN_TARGET,
                           comparison_summary_key)

        # extra in target details
        self.union_details(self.build_details(extra_in_target, comparison_summary_key, False))

    def build_details(self, records, comparison_summary_key, add_count_column=True):
        records = records \
            .withColumn(COMPARISON_DETAILS_KEY, monotonically_increasing_id() + (int(comparison_summary_key) + 1)) \
            .withColumn(COMPARISON_SUMMARY_KEY, lit(comparison_summary_key)) \
            .withColumn(UNIQUE_ROW_KEY, pyspark.sql.functions.concat_ws(',', *self.source_unique_key_array)) \
            .withColumn(CONTEXT, lit(BLANK).cast(StringType())) \
            .withColumn(TIME_CREATED, lit(self.time_created))

        if add_count_column:
            records = records.withColumn(SOURCE_COUNT, lit(ZERO).cast(IntegerType())) \
                .withColumn(TARGET_COUNT, lit(ZERO).cast(IntegerType()))

        return records

    def get_sample_record(self, category_records):
        category_records_sample = category_records.select(
            pyspark.sql.functions.concat_ws('|', *self.source_unique_key_array)) \
            .distinct().take(SAMPLE_SIZE)

        category_records_sample_array = []
        for row in category_records_sample:
            category_records_sample_array.append(str(row[ZERO]))

        return category_records_sample_array

    def build_summary(self, category_records, category_records_sample_array, category_name, comparison_summary_key,
                      comparison_direction=SOURCE_TO_TARGET):
        sample = BLANK
        if len(category_records_sample_array) > 0:
            sample = COMMA.join(category_records_sample_array)

        row_value = [comparison_summary_key, self.job_id, self.sourceEntityName, self.targetEntityName,
                     self.source_unique_key,
                     category_name,
                     category_records.count(), comparison_direction, sample, self.time_created]
        self.summary = self.summary.union(getSparkSession().createDataFrame([row_value], summary_schema()))

    def union_details(self, category_records):
        self.details = self.details.union(category_records.select(COMPARISON_DETAILS_KEY, COMPARISON_SUMMARY_KEY,
                                                                  UNIQUE_ROW_KEY, CONTEXT, SOURCE_COUNT,
                                                                  TARGET_COUNT, TIME_CREATED))


def summary_schema():
    schema = StructType([
        StructField(COMPARISON_SUMMARY_KEY, StringType(), True),
        StructField(JOB_ID, StringType(), True),
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
        StructField(TIME_CREATED, TimestampType(), True)
    ])
    return schema
