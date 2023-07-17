from src.app_context import AppContext
from src import rule_executor


def execute(run_time_parameters):
    context = AppContext(run_time_parameters)
    context.build()

    for rule in context.get_rules():
        rule_executor.execute(rule, context)


