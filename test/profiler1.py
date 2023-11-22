from pyspark.sql import SparkSession, Row, Window
import os

from pyspark.sql.functions import row_number, lit, expr, length, desc
from pyspark.sql.types import StructType, StructField

os.environ['SPARK_VERSION'] = '3.3.2'
import pydeequ
from pydeequ.analyzers import *

spark = (SparkSession
         .builder
         .config("spark.jars.packages", pydeequ.deequ_maven_coord)
         .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
         .getOrCreate())

df = spark.sparkContext.parallelize([
    Row(a="foo", b=1, c=5),
    Row(a="1", b=2, c=6),
    Row(a="1", b="2", c=6),
    Row(a="1.1", b=3.3, c=None)]).toDF()

analysisResult = AnalysisRunner(spark) \
    .onData(df) \
    .addAnalyzer(Size()) \
    .addAnalyzer(Completeness("b")) \
    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
# analysisResult_df.show()

from pydeequ.profiles import *


def calculate_mode(df, column_name):
    grouped_df=df.groupBy(column_name).count()
    sorted_df = grouped_df.orderBy(desc('count'))
    return sorted_df.first()[column_name]


result = ColumnProfilerRunner(spark) \
    .onData(df) \
    .run()

columns = []
column_profiles = []

total_count = df.count()
print('total_count', total_count)
profile_details = {}


def calculate_median(df, column_name, total_count):
    middle_index = int((total_count - 1) / 2)
    window_spec = Window.orderBy(df[column_name]).rowsBetween(-middle_index, middle_index)
    df_with_row_num = df.withColumn("row_num", row_number().over(window_spec)).withColumn("total_count",lit(total_count))
    median_value = df_with_row_num \
        .filter((col("row_num") == (col("total_count") + 1) / 2) |
                ((col("row_num") == (col("total_count") + 2) / 2) & (col("total_count") % 2 == 0))) \
        .select("column_name") \
        .agg(expr("percentile_approx(column_name, 0.5)").alias("median")) \
        .collect()[0]["median"]

    return median_value


def calculate_length(df, column_name):
    df1 = df.withColumn('converted'+column_name, df[column_name].cast("string"))
    df1.show()
    df1=df1.withColumn('length',length(df1['converted' + column_name]))
    df1.show()
    avg = df1.agg({"length": "avg"}).collect()[0]['avg(length)']
    min = df1.agg({"length": "min"}).collect()[0]['min(length)']
    max = df1.agg({"length": "max"}).collect()[0]['max(length)']

    return avg,min,max



def calculate_outliers(df, column_name, total_count):
    quartiles = df.approxQuantile(column_name, [0.25, 0.75], 0.05)
    q1, q3 = quartiles
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    outliers_count = df.filter((col(column_name) < lower_bound) | (col(column_name) > upper_bound)).count()
    return (outliers_count / total_count) * 100


for col, profile in result.profiles.items():
    profile_details = {}
    columns.append(col)
    column_profile = json.loads(str(profile).split(':', 2)[2])
    column_profile['column_name'] = col
    column_profiles.append(column_profile)
    column_name = col
    profile_details['ATTRIBUTE_NAME'] = col
    profile_details['NULL_PERCENTAGE'] = 1 - column_profile['completeness']
    profile_details['UNIQUE_PERCENTAGE'] = (column_profile['approximateNumDistinctValues'] / total_count) * 100
    profile_details['DISTINCT_VALUES_COUNT'] = column_profile['approximateNumDistinctValues']
    profile_details['EXISTING_DATA_TYPE'] = column_profile['dataType']
    profile_details['IDENTIFIED_DATA_TYPE'] = column_profile['dataType']

    if column_profile['dataType'] == 'Integral':
        profile_details['MIN_VALUE'] = column_profile['minimum']
        profile_details['MAX_VALUE'] = column_profile['maximum']
        profile_details['RANGE'] = column_profile['maximum'] - column_profile['minimum']
        profile_details['AVERAGE_VALUE'] = column_profile['mean']
        profile_details['MODE'] = calculate_mode(df.select(column_name), column_name)
        #profile_details['MEDIAN'] = calculate_median(df.select(column_name), column_name, total_count)
        profile_details['MEDIAN'] = calculate_mode(df.select(column_name), column_name)
        profile_details['STD_DEVIATION'] = column_profile['stdDev']
        #profile_details['OUTLIER_PER'] = calculate_outliers(df, column_name, total_count)

    min_length, max_length, average_length = calculate_length(df.select(column_name), column_name)

    profile_details['MIN_LENGTH'] = min_length
    profile_details['MAX_LENGTH'] = max_length
    profile_details['AVERAGE_LENGTH'] = average_length

    print(profile_details)