import sys
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
    "user": "as2408",
    "password": "kn2HR26oLjZx",
    "driver": "org.postgresql.Driver"
}

# Initialize Spark session
spark = SparkSession.builder.appName("SDTM_Transformation") \
    .config("spark.jars") \
    .getOrCreate()

# DataRecode function
@udf(StringType())
def recodeData(value):
    try:
        if value == 'M':
            return 'Male'
        if value == 'F':
            return 'Female'
        raise ValueError(f"Invalid recode value: {value}")
    except Exception as e:
        print(f"ERROR: Recode transformation failed - {str(e)}")
        sys.exit(1)

# Encryption function
@udf(StringType())
def encrypt_value(value):
    try:
        return hashlib.sha256(value.encode()).hexdigest() if value else None
    except Exception as e:
        print(f"ERROR: Encryption failed for value '{value}' - {str(e)}")
        sys.exit(1)

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
    print(f"ERROR: Date skewing failed for value '{value}' - Unrecognized format")
    sys.exit(1)

# Age generalization function
@udf(StringType())
def generalize_age(value):
    try:
        if value is None:
            return None
        value = int(value)
        return f"{(value // 10) * 10}-{((value // 10) * 10) + 9}"
    except ValueError:
        print(f"ERROR: Age generalization failed for value '{value}' - Non-numeric")
        sys.exit(1)

# Load transformation rules
try:
    df_rules = spark.read.format("jdbc") \
        .option("url", KOSH_CONFIG["url"]) \
        .option("dbtable", "test_data_profile.transformations_rules_model") \
        .option("user", KOSH_CONFIG["user"]) \
        .option("password", KOSH_CONFIG["password"]) \
        .option("driver", KOSH_CONFIG["driver"]) \
        .load()
except Exception as e:
    print(f"ERROR: Failed to load transformation rules - {str(e)}")
    sys.exit(1)

if df_rules.rdd.isEmpty():
    print("ERROR: No transformation rules found in database!")
    sys.exit(1)

rules = df_rules.select("table_name", "column_name", "transformations").collect()
transformation_dict = {}
for row in rules:
    table = row["table_name"].replace("sdtm.", "")
    transformation_dict.setdefault(table, {}).setdefault(row["transformations"], []).append(row["column_name"].lower())

# Get all tables in 'newdata' schema
try:
    tables_df = spark.read.format("jdbc") \
        .option("url", SOURCE_CONFIG["url"]) \
        .option("query", "SELECT table_name FROM information_schema.tables WHERE table_schema = 'newdata'") \
        .option("user", SOURCE_CONFIG["user"]) \
        .option("password", SOURCE_CONFIG["password"]) \
        .option("driver", SOURCE_CONFIG["driver"]) \
        .load()
except Exception as e:
    print(f"ERROR: Failed to retrieve table list from newdata schema - {str(e)}")
    sys.exit(1)

tables = [row["table_name"] for row in tables_df.collect()]

if not tables:
    print("ERROR: No tables found in newdata schema!")
    sys.exit(1)

# Process tables
def process_table(table):
    if table not in transformation_dict:
        print(f"WARNING: Skipping table '{table}', no transformation rules.")
        return

    try:
        df = spark.read.format("jdbc") \
            .option("url", SOURCE_CONFIG["url"]) \
            .option("dbtable", f"newdata.{table}") \
            .option("user", SOURCE_CONFIG["user"]) \
            .option("password", SOURCE_CONFIG["password"]) \
            .option("driver", SOURCE_CONFIG["driver"]) \
            .load()
    except Exception as e:
        print(f"ERROR: Failed to read table '{table}' from database - {str(e)}")
        sys.exit(1)

    table_columns = set(df.columns)

    for transformation, columns in transformation_dict[table].items():
        for col_name in columns:
            if col_name not in table_columns:
                print(f"ERROR: Column '{col_name}' does not exist in table '{table}', but is listed in transformation rules.")
                sys.exit(1)

            try:
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

            except Exception as e:
                print(f"ERROR: Transformation '{transformation}' failed for column '{col_name}' in table '{table}' - {str(e)}")
                sys.exit(1)

    print(f"INFO: Successfully transformed '{table}'. Preview:")
    df.show(5)

    try:
        df.write.format("jdbc") \
            .option("url", SOURCE_CONFIG["url"]) \
            .option("dbtable", f"transformeddata.{table}") \
            .option("user", SOURCE_CONFIG["user"]) \
            .option("password", SOURCE_CONFIG["password"]) \
            .option("driver", SOURCE_CONFIG["driver"]) \
            .mode("overwrite") \
            .save()
    except Exception as e:
        print(f"ERROR: Failed to write transformed table '{table}' - {str(e)}")
        sys.exit(1)

    print(f"INFO: Successfully transformed and saved table '{table}'.")

for table in tables:
    process_table(table)

spark.stop()
