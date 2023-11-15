from src.constants import *
from src.reader import read
from src.utils import get_spark_session, get_empty_data_frame, get_unique_id, get_current_time
import json


class SchemaComparator:
    def __init__(self, context):
        self.context = context
        self.rule = self.context.get_current_rule()
        self.context = context
        self.rule = self.context.get_current_rule()
        self.job_id = None
        self.rule_id = None
        self.time_created = get_current_time()
        self.source_unique_key = None
        self.source_unique_key_array = None
        self.target_unique_key = None
        self.target_unique_key_array = None
        self.source_entity_name = None
        self.target_entity_name = None
        self.summary = get_empty_data_frame(summary_schema())
        self.details = get_empty_data_frame(details_schema())
        self.source_entity_name = None
        self.target_entity_name = None
        self.results = {}

    def execute(self):
        source_entity = self.context.get_source_entity()
        target_entity = self.context.get_target_entity()
        self.source_unique_key = source_entity['primary_key']
        self.target_unique_key = target_entity['primary_key']
        self.source_unique_key_array = self.source_unique_key.split(',')
        self.target_unique_key_array = self.target_unique_key.split(',')

        self.source_entity_name = source_entity['entity_name']
        self.target_entity_name = target_entity['entity_name']

        self.job_id = self.context.get_job_run_id()
        self.rule_id = self.context.get_rule_id()
        source_query = f"select * from {self.source_entity_name} where 1==2"
        target_query = f"select * from {self.target_entity_name} where 1==2"
        self.results['source_query'] = source_query
        self.results['target_query'] = target_query
        source_query_execution_start_time = get_current_time()
        source = read(source_entity, source_query, self.context)
        self.results['source_query_end_time'] = get_current_time()
        self.results['source_query_start_time'] = source_query_execution_start_time

        target_query_execution_start_time = get_current_time()
        target = read(target_entity, target_query, self.context)
        self.results['target_query_end_time'] = get_current_time()
        self.results['target_query_start_time'] = target_query_execution_start_time

        summary, details = self.compare_schema(source, target)
        self.results['comparison_summary'] = summary
        self.results['comparison_details'] = details
        self.results['is_data_diff'] = True
        return self.results

    def compare_schema(self, source, target):
        source_columns = set(source.columns)
        target_columns = set(target.columns)

        # Find columns missing in source
        missing_in_source = target_columns - source_columns

        # Find columns missing in target
        missing_in_target = source_columns - target_columns

        mismatched_data_types = []
        for col in source_columns.intersection(target_columns):
            if source_columns[col].dataType != target_columns[col].dataType:
                mismatched_data_types.append((col, source_columns[col].dataType, target_columns[col].dataType))

        summary_row_list = []

        summary_row_list[0] = [get_unique_id(), self.job_id, self.rule_id, self.source_entity_name,
                               self.target_entity_name,
                               self.source_unique_key,
                               'COLUMN_MISSING_IN_SOURCE',
                               len(missing_in_source), 'SOURCE_TO_TARGET', missing_in_source[:5], self.time_created]

        summary_row_list[1] = [get_unique_id(), self.job_id, self.rule_id, self.source_entity_name,
                               self.target_entity_name,
                               self.source_unique_key,
                               'COLUMN_MISSING_IN_TARGET',
                               len(missing_in_target), 'SOURCE_TO_TARGET', missing_in_target[:5], self.time_created]

        columns_with_mismatched_data_types = [entry[0] for entry in mismatched_data_types]

        summary_row_list[2] = [get_unique_id(), self.job_id, self.rule_id, self.source_entity_name,
                               self.target_entity_name,
                               self.source_unique_key,
                               'COLUMN_MATCHED_DATA_TYPE_MISMATCHED',
                               len(mismatched_data_types), 'SOURCE_TO_TARGET', columns_with_mismatched_data_types[:5],
                               self.time_created]

        summary_row_list[3] = [get_unique_id(), self.job_id, self.rule_id, self.source_entity_name,
                               self.target_entity_name,
                               self.source_unique_key,
                               'SOURCE_COLUMN_COUNT',
                               len(source_columns), 'SOURCE_TO_SOURCE', BLANK, self.time_created]

        summary_row_list[4] = [get_unique_id(), self.job_id, self.rule_id, self.source_entity_name,
                               self.target_entity_name,
                               self.source_unique_key,
                               'TARGET_COLUMN_COUNT',
                               len(target_columns), 'TARGET_TO_TARGET', BLANK, self.time_created]

        self.summary = self.summary.union(
                get_spark_session().createDataFrame(summary_row_list, summary_schema()))

        details_rows = []

        for column in missing_in_source:
            details_rows.append(
                [get_unique_id(), get_unique_id(), self.target_unique_key + "." + column, column, self.time_created])

        for column in missing_in_target:
            details_rows.append(
                [get_unique_id(), get_unique_id(), self.source_unique_key + "." + column, column, self.time_created])

        for entry in mismatched_data_types:
            context_map = {'column_name': entry[0], 'source_column_data_type': entry[1],
                           'target_column_data_type': entry[2]}
            details_rows.append(
                [get_unique_id(), get_unique_id(),
                 self.source_unique_key + "." + self.target_unique_key + "." + entry[0],
                 json.dumps(context_map, indent=4), self.time_created])

        self.details = self.details.union(get_spark_session().createDataFrame(details_rows, details_schema()))

        return  self.summary , self.details
