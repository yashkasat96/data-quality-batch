from reader import read


class RangeCheck:
    def __init__(self, context):
        self.rule = None
        self.context = context

    def execute(self, rule):
        self.rule = rule
        column_name = self.context.get_rule_property('COLUMN_NAME', self.rule)
        filter_condition = self.context.get_rule_property('FILTER_CONDITIONS', self.rule)
        base_criteria = self.context.get_rule_property('BASE_CRITERIA', self.rule)
        upper_limit = self.context.get_rule_property('UPPER_LIMIT', self.rule)
        lower_limit = self.context.get_rule_property('LOWER_LIMIT', self.rule)
        entity = self.context.get_source_entity(self.rule)
        primary_key = self.context.get_primary_key(entity)
        entity_physical_name = self.context.get_physical_name(entity)

        base_criteria = base_criteria.replace('{COLUMN_NAME}', column_name)
        base_criteria = base_criteria.replace('{UPPER_LIMIT}', column_name)
        base_criteria = base_criteria.replace('{LOWER_LIMIT}', lower_limit)

        failed_records_query = f"select {primary_key} from {entity_physical_name} where {base_criteria} and {filter_condition}"
        failed_records = read(entity, failed_records_query)
        total_records_query = f"select count(*) from {entity_physical_name} where {filter_condition}"
        total_records_count = read(entity, total_records_query)


