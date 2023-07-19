from datetime import datetime

from src.reader import read


def execute_rule_queries(entity, failed_records_query, total_records_query, context):
    failed_records_query_execution_start_time = datetime.now()
    failed_records = read(entity, failed_records_query, context)
    failed_records_query_execution_end_time = datetime.now()

    total_records_query_execution_start_time = datetime.now()
    total_records_count = read(entity, total_records_query, context)
    total_records_query_execution_end_time = datetime.now()
    primary_key = entity['primary_key']

    return {'failed_records': failed_records,
            'total_records_count': total_records_count.first()['total_count'],
            'failed_records_query': failed_records_query,
            'total_records_query': total_records_query,
            'failed_records_query_execution_start_time': failed_records_query_execution_start_time,
            'failed_records_query_execution_end_time': failed_records_query_execution_end_time,
            'total_records_query_execution_start_time': total_records_query_execution_start_time,
            'total_records_query_execution_end_time': total_records_query_execution_end_time,
            'primary_key': primary_key
            }
