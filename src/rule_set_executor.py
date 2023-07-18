from datetime import datetime

from rules.data_comparator import DataComparator
from rules.sql_validator import SqlValidator


def pre_process():

    pass

class RuleSetExecutor:
    def __init__(self, context):
        self.context = context

    def execute(self):
        execution_result ={}
        execution_result['rule_set_execution_start_time']=datetime.now()
        for rule in self.context.get_rules():
            rule_execution_start_time = datetime.now()
            results = self.get_rule_executor(rule).execute(rule, self.context)
            results['rule_execution_end_time'] = datetime.now()
            results['rule_execution_start_time'] = rule_execution_start_time
            execution_result[self.context.get_rule_id(rule)] = results
        execution_result['rule_set_execution_end_time'] = datetime.now()
        return execution_result

    def get_rule_executor(self,rule):
        template_name = self.context.get_rule_template_name(rule)
        executor = None
        if template_name == 'data_diff':
            executor = DataComparator(self.context)
        if template_name == 'sql_validator':
            executor = SqlValidator(self.context)
        if template_name == 'range_check':
            executor = SqlValidator(self.context)
        if template_name == 'null_check':
            executor = SqlValidator(self.context)
        if template_name == 'natural_language_rule':
            executor = SqlValidator(self.context)
        return executor

