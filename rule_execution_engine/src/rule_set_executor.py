from rule_execution_engine.src.rules.rule_factory import RuleExecutorFactory
from rule_execution_engine.src.utils import get_current_time


class RuleSetExecutor:
    def __init__(self, context):
        self.context = context

    def execute(self):
        execution_result = {'rule_set_execution_start_time': get_current_time()}
        rule_factory = RuleExecutorFactory(self.context)
        for rule in self.context.get_rules():
            self.context.set_current_rule(rule)
            rule_execution_start_time = get_current_time()
            results = rule_factory.get_rule_executor().execute()
            results['rule_execution_end_time'] = get_current_time()
            results['rule_execution_start_time'] = rule_execution_start_time
            execution_result[self.context.get_rule_id()] = results

        execution_result['rule_set_execution_end_time'] = get_current_time()
        return execution_result
