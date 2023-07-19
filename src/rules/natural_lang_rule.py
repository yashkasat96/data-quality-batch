import openai
from langchain import PromptTemplate

from src.query_executor import execute_rule_queries


class NaturalLanguageRule:
    def __init__(self, context):
        self.context = context
        self.rule = self.context.get_current_rule()

    def execute(self):
        statement = self.context.get_rule_property('STATEMENT')
        entity = self.context.get_source_entity(self.rule)
        primary_key = entity['primary_key']
        filter_condition = self.context.get_rule_property('FILTER_CONDITIONS')
        entity_physical_name = entity['entity_physical_name']
        failed_records_query = self.generate_failed_records_query(entity_physical_name, primary_key, statement)
        total_records_query = f"select count(*)  as total_count from {entity_physical_name} where {filter_condition}"
        return execute_rule_queries(entity, failed_records_query, total_records_query,self.context)

    def generate_failed_records_query(self, entity_physical_name, primary_key, statement):
        template = "Create sql query for statement {statement} . Use table name {table_name} . Add column " \
                   "{primary_key} in select clause of query.Add {filter_condition} in where clause.Only return query " \
                   "as output."
        prompt_template = PromptTemplate.from_template(template)
        prompt = prompt_template.format(statement=statement, table_name=entity_physical_name, primary_key=primary_key)
        failed_records_query = self.get_completion(prompt)
        return failed_records_query

    def get_completion(self, prompt):
        openai.api_key = self.context.get_value('open_ai_api_key')
        model = self.context.get_value('model')
        messages = [{"role": "user", "content": prompt}]
        response = openai.ChatCompletion.create(
            model=model,
            messages=messages,
            temperature=0,
        )
        return response.choices[0].message["content"]
