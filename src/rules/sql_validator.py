from reader import read
from rules.rule_query_executor import execute_rule_queries


class SqlValidator:
    def __init__(self, context):
        self.context = context
        self.rule = None

    def execute(self, rule):
        self.rule = rule
        failed_records_query = self.context.get_rule_property('SOURCE_QUERY', self.rule)
        filter_condition = self.context.get_rule_property('FILTER_CONDITIONS', self.rule)
        entity = self.context.get_source_entity(self.rule)
        entity_physical_name = self.context.get_physical_name(entity)
        total_records_query = f"select count(*) from {entity_physical_name} where {filter_condition}"
        return execute_rule_queries(entity, failed_records_query, total_records_query)
