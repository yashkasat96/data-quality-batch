from datetime import datetime

from src.reader import read
from src.utils import get_current_time


def execute_rule_queries(entity, failed_records_query, total_records_query, context):
    failed_records_query_execution_start_time = get_current_time()
    failed_records = read(entity, failed_records_query, context)
    failed_records_query_execution_end_time = get_current_time()

    total_records_query_execution_start_time = get_current_time()
    total_records = read(entity, total_records_query, context)
    total_records_query_execution_end_time = get_current_time()
    primary_key = entity['primary_key']
    threshold_percent = float(context.get_rule_property('THRESHOLD_PERCT')) if context.is_key_exist_in_rule_property('THRESHOLD_PERCT') else 100.00
    total_record_count = total_records.first()['total_count']
    failed_record_count = failed_records.count()
    pass_record_count = total_record_count - failed_record_count
    is_rule_passed = pass_record_count * 100 / total_record_count >= threshold_percent

    return {'failed_records': failed_records,
            'total_records_count': total_record_count,
            'is_rule_passed': is_rule_passed,
            'failed_records_query': failed_records_query,
            'total_records_query': total_records_query,
            'failed_records_query_execution_start_time': failed_records_query_execution_start_time,
            'failed_records_query_execution_end_time': failed_records_query_execution_end_time,
            'total_records_query_execution_start_time': total_records_query_execution_start_time,
            'total_records_query_execution_end_time': total_records_query_execution_end_time,
            'primary_key': primary_key
            }
