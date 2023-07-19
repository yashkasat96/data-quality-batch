from datetime import datetime

from src.rules.rule_factory import RuleFactory


class RuleSetExecutor:
    def __init__(self, context):
        self.context = context

    def execute(self):
        execution_result = {'rule_set_execution_start_time': datetime.now()}
        rule_factory = RuleFactory(self.context)
        for rule in self.context.get_rules():
            self.context.set_current_rule(rule)
            rule_execution_start_time = datetime.now()
            results = rule_factory.get_rule_executor().execute()
            results['rule_execution_end_time'] = datetime.now()
            results['rule_execution_start_time'] = rule_execution_start_time
            execution_result[self.context.get_rule_id()] = results

        execution_result['rule_set_execution_end_time'] = datetime.now()
        return execution_result
