from src.rules.data_comparator import DataComparator
from src.rules.null_check import NullCheck
from src.rules.range_check import RangeCheck
from src.rules.sql_validator import SqlValidator


class RuleExecutorFactory:
    def __init__(self, context):
        self.context = context

    def get_rule_executor(self):
        template_name = self.context.get_rule_template_name()
        executor = None
        if template_name == 'DATA_DIFF':
            executor = DataComparator(self.context)
        if template_name == 'SQL_VALIDATOR':
            executor = SqlValidator(self.context)
        if template_name == 'RANGE_CHECK':
            executor = RangeCheck(self.context)
        if template_name == 'NULL_CHECK':
            executor = NullCheck(self.context)

        return executor
