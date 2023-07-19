from src.query_executor import execute_rule_queries


class SqlValidator:
    def __init__(self, context):
        self.context = context
        self.rule = self.context.get_current_rule()

    def execute(self, rule):
        failed_records_query = self.context.get_rule_property('SOURCE_QUERY')
        filter_condition = self.context.get_rule_property('FILTER_CONDITIONS')
        entity = self.context.get_source_entity()
        entity_physical_name = entity['entity_physical_name']
        total_records_query = f"select count(*) from {entity_physical_name} where {filter_condition}"
        return execute_rule_queries(entity, failed_records_query, total_records_query,self.context)
