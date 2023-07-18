from reader import read
import openai
from langchain import PromptTemplate


class NaturalLanguageRule:
    def __init__(self, context):
        self.context = context
        self.rule = None

    def execute(self, rule):
        self.rule = rule
        statement = self.context.get_rule_property('STATEMENT', self.rule)
        entity = self.context.get_source_entity(self.rule)
        primary_key = self.context.get_primary_key(entity)
        filter_condition = self.context.get_rule_property('FILTER_CONDITIONS', self.rule)
        entity_physical_name = self.context.get_physical_name(entity)
        template = "Create sql query for statement {statement} . Use table name {table_name} . Add column " \
                   "{primary_key} in select clause of query.Add {filter_condition} in where clause.Only return query " \
                   "as output."
        prompt_template = PromptTemplate.from_template(template)
        prompt = prompt_template.format(statement=statement, table_name=entity_physical_name, primary_key=primary_key)
        query = self.get_completion(prompt)
        failed_records = read(entity, query)
        total_records_query = f"select count(*) from {entity_physical_name} where {filter_condition}"
        total_records_count = read(entity, total_records_query)

    def get_completion(self, prompt, model="gpt-3.5-turbo"):
        openai.api_key = self.context.get_value('open_ai_api_key')
        model = self.context.get_value('model')
        messages = [{"role": "user", "content": prompt}]
        response = openai.ChatCompletion.create(
            model=model,
            messages=messages,
            temperature=0,  # this is the degree of randomness of the model's output
        )
        return response.choices[0].message["content"]
