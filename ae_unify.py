from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

# ✅ Initialize Spark Session
spark = SparkSession.builder \
    .appName("SDTM AE Dataset Unification") \
    .config("spark.jars"") \
    .getOrCreate()

# ✅ PostgreSQL Configuration
DB_CONFIG = {
    "url": "jdbc:postgresql://w3.training5.modak.com:5432/postgres",
    "user": "mt24042",
    "password": "mt24042@m06y24",
    "driver": "org.postgresql.Driver"
}

# ✅ List of AE Datasets
datasets = ["dataset_sdtm_ae1", "dataset_sdtm_ae2", "dataset_sdtm_ae3"]

# ✅ Read Schema Information from PostgreSQL
schema_query = f"""
    (SELECT table_name, column_name, data_type 
     FROM information_schema.columns 
     WHERE table_name IN ({', '.join(f"'{table}'" for table in datasets)})) as schema_table
"""

schema_df = spark.read.format("jdbc") \
    .option("url", DB_CONFIG["url"]) \
    .option("dbtable", schema_query) \
    .option("user", DB_CONFIG["user"]) \
    .option("password", DB_CONFIG["password"]) \
    .option("driver", DB_CONFIG["driver"]) \
    .load()

# ✅ Convert Schema to Dictionary
schema_dict = schema_df.rdd.map(lambda row: (row.column_name, row.data_type)).groupByKey().mapValues(set).collectAsMap()

# ✅ Define Data Type Mapping for PySpark
def unify_datatype(types):
    types = set(types)
    
    if "timestamp" in types and "date" in types:
        return TimestampType()
    if "timestamp" in types:
        return TimestampType()
    if "date" in types:
        return DateType()
    if "varchar" in types or "text" in types:
        return StringType()
    if "integer" in types:
        return IntegerType()
    if "double precision" in types or "numeric" in types:
        return DoubleType()
    return StringType()  # Default fallback

# ✅ Generate Unified Schema
unified_schema = StructType([StructField(col, unify_datatype(types), True) for col, types in schema_dict.items()])

# ✅ Read and Standardize Datasets
dfs = []
for dataset in datasets:
    df = spark.read.format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", dataset) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .load()

    # ✅ Ensure all columns exist (add missing ones as NULL)
    for col_name in schema_dict.keys():
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast(unified_schema[col_name].dataType))

    # ✅ Select columns in correct order & cast types
    df = df.select(*[col(c).cast(unified_schema[c].dataType) for c in schema_dict.keys()])
    
    dfs.append(df)

# ✅ Merge DataFrames
unified_df = dfs[0]
for df in dfs[1:]:
    unified_df = unified_df.unionByName(df)

# ✅ Write Unified AE Data Back to PostgreSQL
unified_df.write \
    .format("jdbc") \
    .option("url", DB_CONFIG["url"]) \
    .option("dbtable", "unified_sdtm_dataset_ae_final") \
    .option("user", DB_CONFIG["user"]) \
    .option("password", DB_CONFIG["password"]) \
    .option("driver", DB_CONFIG["driver"]) \
    .mode("overwrite") \
    .save()

print("🎉 Unified AE dataset successfully stored in PostgreSQL!")
