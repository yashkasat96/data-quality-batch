from dq_rule_execution_engine.src.reader import read
from dq_rule_execution_engine.src.utils import get_current_time


def execute_rule_queries(entity, failed_records_query, total_records_query, context):
    failed_records_query_results = execute_failed_records_query(
         entity, failed_records_query,context)
    total_records_query_results = execute_total_records_query(entity, total_records_query,context)
    return { ** failed_records_query_results , ** total_records_query_results , 'primary_key': entity['primary_key']}


def execute_failed_records_query(entity, failed_records_query,context):
    failed_records_query_execution_start_time = get_current_time()
    failed_records = read(entity, failed_records_query, context)
    failed_records_query_execution_end_time = get_current_time()
    return {'failed_records': failed_records,
            'failed_records_query': failed_records_query,
            'failed_records_query_execution_start_time': failed_records_query_execution_start_time,
            'failed_records_query_execution_end_time': failed_records_query_execution_end_time,
            }


def execute_total_records_query(entity, total_records_query,context):
    total_records_query_execution_start_time = get_current_time()
    total_records_count = read(entity, total_records_query, context)
    total_records_query_execution_end_time = get_current_time()
    return {'total_records_count': total_records_count.first()['total_count'],
            'total_records_query': total_records_query,
            'total_records_query_execution_start_time': total_records_query_execution_start_time,
            'total_records_query_execution_end_time': total_records_query_execution_end_time,
            }

