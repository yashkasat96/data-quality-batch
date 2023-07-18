from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.execution_results_writer import ExecutionResultsWriter
from src.rule_set_executor import RuleSetExecutor
from src.app_context import AppContext


def execute(run_time_parameters):
    context = AppContext(run_time_parameters)
    context.build()
    result = RuleSetExecutor(context).execute()
    ExecutionResultsWriter(context).write(result)
