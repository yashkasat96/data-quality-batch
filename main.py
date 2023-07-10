from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType
from pathlib import Path
import json
import datetime
import time
import os

spark = SparkSession \
        .builder \
        .appName("Data-Quality") \
        .master("local[*]") \
        .getOrCreate()

# read the data from the csv file
def get_df(file_path):
    df = spark.read.csv(file_path, header=True)
    df = df.cache()
    return df


def get_data(rule):
    if rule['rule_details']['data_entity_associations'][0]['data_entity_details']['type'] == 'file':
        # print("-------------------------------------------------FILE--------------------------------------------------------------")
        # Loading a dataset from the source path specified in config json and the fileType in config json
        requestedDataset = spark.read.load(rule['rule_details']['data_entity_associations'][0]['data_entity_details']['location'],
                                           format=rule['rule_details']['data_entity_associations'][0]['data_entity_details']['sub_type'], header='True',
                                           escape='"', dateFormat="yyyy-MM-dd")

    elif rule['rule_details']['data_entity_associations'][0]['data_entity_details']['type'] == 'hive':
        pass

    elif rule['rule_details']['data_entity_associations'][0]['data_entity_details']['type'] == 'RDBMS/database':
        pass

    return requestedDataset

def execute_sql_template_rule(input_df, rule, df_parameters_rule_run_stats, rule_run_stats_df_schema):
    rule_id = rule['rule_details']['id']
    input_df.createOrReplaceTempView(rule['rule_details']['data_entity_associations'][0]['data_entity_details']['physical_name'])
    total_records = input_df.count()
    output_df = spark.sql(rule['rule_details']['properties'][0]['value'])
    pass_count = output_df.count()
    fail_count = total_records - pass_count

    rule_run_stats_list = [df_parameters_rule_run_stats['ruleset_id'],
                           rule_id,
                           df_parameters_rule_run_stats['job_run_id'],
                           total_records,
                           fail_count,
                           pass_count,
                           df_parameters_rule_run_stats['created_time']]

    rule_run_stats_df = spark.createDataFrame([rule_run_stats_list], rule_run_stats_df_schema)
    return rule_run_stats_df


# defines the schema of the rule_run_stats output dataframe
def get_schema_of_rule_run_stats_df():
    schema = StructType([
        StructField('ruleset_id', IntegerType(), True),
        StructField('rule_id', IntegerType(), True),
        StructField('job_run_id', StringType(), True),
        StructField('total_records', IntegerType(), True),
        StructField('fail_count', IntegerType(), True),
        StructField('pass_count', IntegerType(), True),
        StructField('created_time', TimestampType(), True)
        # StructField('RESULTS', ArrayType(StringType()), True),
        # StructField('TIME_CREATED', TimestampType(), True)
    ])
    return schema

# defines the schema of the run_stats output dataframe
def get_schema_of_run_stats_df():
    schema = StructType([
        StructField('ruleset_id', IntegerType(), True),
        StructField('rule_id', IntegerType(), True),
        StructField('job_run_id', StringType(), True),
        StructField('start_date', TimestampType(), True),
        StructField('end_date', TimestampType(), True),
        StructField('created_time', TimestampType(), True)
    ])
    return schema

# defines the schema of the query_stats output dataframe
def get_schema_of_query_stats_df():
    schema = StructType([
        StructField('rule_id', IntegerType(), True),
        StructField('job_run_id', StringType(), True),
        StructField('query', StringType(), True),
        StructField('start_date', IntegerType(), True),
        StructField('end_date', IntegerType(), True),
        StructField('total_execution_time', IntegerType(), True)
    ])
    return schema

# create an empty dataframe
def get_empty_summarydf(schema):
    """
    create empty dataframe for comparison summary
    """
    df = spark.createDataFrame(data=[], schema=schema)
    return df


def main():

    job_id = spark.sparkContext.applicationId
    date_of_creation = datetime.datetime.now()

    # Reading JSON Template File
    with open('C:\\Users\\Shantanu201880\\Documents\\InnerSource_Data_Quality\\Resources\\JSON_Rule_Template_New.json', 'r') as config_file:
        config_data = json.load(config_file)

    ruleset_id = config_data['id']

    rule_run_stats_df_schema = get_schema_of_rule_run_stats_df()
    consolidated_rule_run_stats_df = get_empty_summarydf(rule_run_stats_df_schema)

    df_parameters_rule_run_stats = {'ruleset_id': ruleset_id,
                                    'job_run_id': job_id,
                                    'created_time': date_of_creation,
                                    }

    run_stats_df_schema = get_schema_of_run_stats_df()
    consolidated_run_stats_df = get_empty_summarydf(run_stats_df_schema)

    df_parameters_run_stats = {'ruleset_id': ruleset_id,
                                'job_run_id': job_id,
                                'created_time': date_of_creation,
                                }

    query_stats_df_schema = get_schema_of_query_stats_df()
    consolidated_query_stats_df = get_empty_summarydf(query_stats_df_schema)

    # df_parameters_query_stats

    for rule in range(len(config_data['rules'])):
        # print(config_data['rules'][rule])
        # Create a function to identify 'entity_type' like File/Database etc.
        if config_data['rules'][rule]['rule_details']['properties'][0]['name'] == 'sourceQuery':
            rule_run_start_date = datetime.datetime.now()
            data_df = get_data(config_data['rules'][rule])
            # data_df.show(truncate=False)

            # Start the timer
            query_start_time = int(time.time())

            output_df = execute_sql_template_rule(data_df, config_data['rules'][rule], df_parameters_rule_run_stats,
                                      rule_run_stats_df_schema)
            consolidated_rule_run_stats_df = consolidated_rule_run_stats_df.union(output_df)

            # Stop the timer
            query_end_time = int(time.time())

            # Calculate the execution time
            query_execution_time = int(query_end_time - query_start_time)

            query_stats_list = [config_data['rules'][rule]['rule_details']['id'],
                                job_id,
                                config_data['rules'][rule]['rule_details']['properties'][0]['value'],
                                query_start_time,
                                query_end_time,
                                query_execution_time]

            query_stats_df = spark.createDataFrame([query_stats_list], query_stats_df_schema)
            consolidated_query_stats_df = consolidated_query_stats_df.union(query_stats_df)

            rule_run_end_date = datetime.datetime.now()
            run_stats_list = [df_parameters_run_stats['ruleset_id'],
                              config_data['rules'][rule]['rule_details']['id'],
                              df_parameters_run_stats['job_run_id'],
                              rule_run_start_date,
                              rule_run_end_date,
                              df_parameters_run_stats['created_time']]
            run_stats_df = spark.createDataFrame([run_stats_list], run_stats_df_schema)
            consolidated_run_stats_df = consolidated_run_stats_df.union(run_stats_df)

    print('-------------------JOB RUN STATS-------------------')
    consolidated_run_stats_df.show(truncate=False)

    print('-------------------RULE RUN STATS-------------------')
    consolidated_rule_run_stats_df.show(truncate=False)

    print('-------------------QUERY RUN STATS-------------------')
    consolidated_query_stats_df.show(truncate=False)



if __name__ == "__main__":
    main()