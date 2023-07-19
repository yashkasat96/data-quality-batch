from rules.data_comparator import DataComparator
from rules.natural_lang_rule import NaturalLanguageRule
from rules.null_check import NullCheck
from rules.range_check import RangeCheck
from rules.sql_validator import SqlValidator


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
        if template_name == 'NATURAL_LANG_RULE':
            executor = NaturalLanguageRule(self.context)

        return executor
