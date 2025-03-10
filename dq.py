from operator import truediv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import StringType, IntegerType
import hashlib
from datetime import datetime, timedelta

# Database configuration
SOURCE_CONFIG = {
    "url": "jdbc:postgresql://w3.training5.modak.com:5432/source_db",
    "user": "mb2510",
    "password": "dD2M5fTklT5K",
    "driver": "org.postgresql.Driver"
}

KOSH_CONFIG = {
    "url": "jdbc:postgresql://testnabu-3.modak.com:5432/kosh",
    "user": "as2408",
    "password": "kn2HR26oLjZx",
    "driver": "org.postgresql.Driver"
}

# Initialize Spark session
spark = SparkSession.builder.appName("SDTM_QualityControl") \
    .config("spark.jars", "/home/mt24042/Downloads/postgresql-42.5.1.jar") \
    .getOrCreate()


# DataRecode function
@udf(StringType())
def recodeData(sourcevalue, transformedvalue):
    if transformedvalue == 'Male':
        value = 'M'
    if transformedvalue == 'Female':
        value = 'F'
    print(f"'{sourcevalue}' '{value}' '{transformedvalue}'")
    if sourcevalue == value:
        return 'record validated'
    else:
        return 'record is not matched with source'


# Encryption function
@udf(StringType())
def encrypt_value(sourcevalue, transformedvalue):
    value = hashlib.sha256(sourcevalue.encode()).hexdigest()
    print(f"'{sourcevalue}' '{value}' '{transformedvalue}'")
    if value == transformedvalue:
        return 'record validated'
    else:
        return 'record is not matched with source'


# Date skew function
@udf(StringType())
def skew_date(sourcevalue, transformedvalue):
    date_formats = ["%d/%m/%y", "%Y-%m-%d", "%d-%b-%Y"]
    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(sourcevalue, fmt)
            value = (date_obj + timedelta(days=5)).strftime(fmt)
        except ValueError:
            continue
    print(f"'{sourcevalue}' '{value}' '{transformedvalue}'")
    if value == transformedvalue:
        return 'record validated'
    else:
        return 'record is not matched with source'


# Age generalization function
@udf(StringType())
def generalize_age(sourcevalue, transformedvalue):
    try:
        value = int(sourcevalue)
        value = f"{(value // 10) * 10}-{((value // 10) * 10) + 9}"
    except ValueError:
        return None
    print(f"'{sourcevalue}' '{value}' '{transformedvalue}'")
    if value == transformedvalue:
        return 'record validated'
    else:
        return 'record is not matched with source'


# Load transformation rules
df_rules = spark.read.format("jdbc") \
    .option("url", KOSH_CONFIG["url"]) \
    .option("dbtable", "test_data_profile.transformations_rules_model") \
    .option("user", KOSH_CONFIG["user"]) \
    .option("password", KOSH_CONFIG["password"]) \
    .option("driver", KOSH_CONFIG["driver"]) \
    .load()

rules = df_rules.select("table_name", "column_name", "transformations").collect()
transformation_dict = {}
for row in rules:
    # table = row["table_name"].replace("sdtm.", "")
    transformation_dict.setdefault(row["table_name"], {}).setdefault(row["transformations"], []).append(
        row["column_name"].lower())

# Get all tables in 'sdtm' schema
tables_df = spark.read.format("jdbc") \
    .option("url", SOURCE_CONFIG["url"]) \
    .option("query", "SELECT table_name FROM information_schema.tables WHERE table_schema = 'sdtm_transformed'") \
    .option("user", SOURCE_CONFIG["user"]) \
    .option("password", SOURCE_CONFIG["password"]) \
    .option("driver", SOURCE_CONFIG["driver"]) \
    .load()
tables = [row["table_name"] for row in tables_df.collect()]


# Process tables
def process_table(table):
    transformedDatadf = spark.read.format("jdbc") \
        .option("url", SOURCE_CONFIG["url"]) \
        .option("dbtable", f"sdtm_transformed.{table}") \
        .option("user", SOURCE_CONFIG["user"]) \
        .option("password", SOURCE_CONFIG["password"]) \
        .option("driver", SOURCE_CONFIG["driver"]) \
        .load()
    transformed_table_columns = set(transformedDatadf.columns)

    sdtmDatadf = spark.read.format("jdbc") \
        .option("url", SOURCE_CONFIG["url"]) \
        .option("dbtable", f"sdtm.{table}") \
        .option("user", SOURCE_CONFIG["user"]) \
        .option("password", SOURCE_CONFIG["password"]) \
        .option("driver", SOURCE_CONFIG["driver"]) \
        .load()

    ##checking if the row count is same between source and transformed
    if transformedDatadf.count() == sdtmDatadf.count():
        print(f"source and transformed tables row count are equal for {table}")
    else:
        print(f"some data is missed while performing transformation of {table}")

    for col_name in transformed_table_columns:
        if transformedDatadf.filter(F.col(col_name).isNull()).count() == 0:
            print(f"there is no null data in {col_name} of {table}")
        else:
            print(f"there is null data in {col_name} of {table} please check the data")

    print(f"transformed data of '{table}' is verified and as expected")


for table in tables:
    process_table(table)

spark.stop()