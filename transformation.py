from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import StringType, IntegerType
import hashlib
from datetime import datetime, timedelta

# Database configuration
SOURCE_CONFIG = {
    "url": "jdbc:postgresql://w3.training5.modak.com:5432/source_db",
    "user": "mt24042",
    "password": "mt24042@m06y24",
    "driver": "org.postgresql.Driver"
}

KOSH_CONFIG = {
    "url": "jdbc:postgresql://testnabu-3.modak.com:5432/kosh",
    "user1": "as2408",
    "password1": "kn2HR26oLjZx",
    "driver": "org.postgresql.Driver"
}

# Initialize Spark session
spark = SparkSession.builder.appName("SDTM_Transformation") \
    .config("spark.jars", "/home/mt24042/Downloads/postgresql-42.5.1.jar") \
    .getOrCreate()


# DataRecode function
@udf(StringType())
def recodeData(value):
    if value == 'M' :
        return 'Male'
    if value == 'F' :
        return 'Female'


# Encryption function
@udf(StringType())
def encrypt_value(value):
    return hashlib.sha256(value.encode()).hexdigest() if value else None


# Date skew function
@udf(StringType())
def skew_date(value):
    if not value:
        return None
    date_formats = ["%d/%m/%y", "%Y-%m-%d", "%d-%b-%Y"]
    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(value, fmt)
            return (date_obj + timedelta(days=5)).strftime(fmt)
        except ValueError:
            continue
    return value


# Age generalization function
@udf(StringType())
def generalize_age(value):
    if value is None:
        return None
    try:
        value = int(value)
        return f"{(value // 10) * 10}-{((value // 10) * 10) + 9}"
    except ValueError:
        return None


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
    table = row["table_name"].replace("sdtm.", "")
    transformation_dict.setdefault(table, {}).setdefault(row["transformations"], []).append(row["column_name"].lower())

# Get all tables in 'sdtm' schema
tables_df = spark.read.format("jdbc") \
    .option("url", SOURCE_CONFIG["url"]) \
    .option("query", "SELECT table_name FROM information_schema.tables WHERE table_schema = 'newdata'") \
    .option("user", SOURCE_CONFIG["user"]) \
    .option("password", SOURCE_CONFIG["password"]) \
    .option("driver", SOURCE_CONFIG["driver"]) \
    .load()
tables = [row["table_name"] for row in tables_df.collect()]


# Process tables
def process_table(table):
    if table not in transformation_dict:
        print(f"Skipping table '{table}', no transformation rules.")
        return

    df = spark.read.format("jdbc") \
        .option("url", SOURCE_CONFIG["url"]) \
        .option("dbtable", f"newdata.{table}") \
        .option("user", SOURCE_CONFIG["user"]) \
        .option("password", SOURCE_CONFIG["password"]) \
        .option("driver", SOURCE_CONFIG["driver"]) \
        .load()
    table_columns = set(df.columns)

    for transformation, columns in transformation_dict[table].items():
        for col_name in columns:
            if col_name not in table_columns:
                print(f"Skipping column '{col_name}' in table '{table}', does not exist.")
                continue

            if transformation == "encrypt":
                df = df.withColumn(col_name, encrypt_value(col(col_name).cast(StringType())))
            elif transformation == "skew":
                df = df.withColumn(col_name, skew_date(col(col_name)))
            elif transformation == "generalize":
                df = df.withColumn(col_name, generalize_age(col(col_name).cast(IntegerType())))
            elif transformation == "redact":
                df = df.withColumn(col_name, when(col(col_name).isNotNull(), "").otherwise(col(col_name)))
            elif transformation == "recode":
                df = df.withColumn(col_name, recodeData(col(col_name)))

    print(f"Preview of transformed '{table}' data:")
    df.show(5)

    df.write.format("jdbc") \
        .option("url", SOURCE_CONFIG["url"]) \
        .option("dbtable", f"transformeddata.{table}") \
        .option("user", SOURCE_CONFIG["user"]) \
        .option("password", SOURCE_CONFIG["password"]) \
        .option("driver", SOURCE_CONFIG["driver"]) \
        .mode("overwrite") \
        .save()

    print(f"Successfully transformed and saved table: {table}")


for table in tables:
    process_table(table)

spark.stop()