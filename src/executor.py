from src.app_context import AppContext
from src.execution_results_writer import ExecutionResultsWriter
from src.rule_set_executor import RuleSetExecutor


def execute(run_time_parameters):
    context = AppContext(run_time_parameters)
    context.build()
    result = RuleSetExecutor(context).execute()
    ExecutionResultsWriter(context).write(result)
