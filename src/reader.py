from src.utils import getSparkSession


def csv(entity, query):
    location = [x for x in entity['properties'] if x['key'] == 'location'][0]['value']
    data = getSparkSession().read.csv(location, header=True)
    data.registerTempTable(entity['entity_name'])
    return getSparkSession().sql(query)


def read(entity, query):
    entity_sub_type = entity['entity_sub_type']
    data = None
    if entity_sub_type == 'csv':
        data = csv(entity, query)
    return data
