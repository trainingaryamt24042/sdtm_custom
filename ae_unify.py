from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Unify SDTM AE Datasets").getOrCreate()

# Define PostgreSQL connection details
postgres_url = "jdbc:postgresql://w3.training5.modak.com:5432/postgres"
postgres_properties = {
    "user": "mt24042",
    "password": "mt24042@m06y24",
    "driver": "org.postgresql.Driver"
}

# Define unified schema for the datasets
unified_schema = StructType([
    StructField("USUBJID", StringType(), nullable=False),
    StructField("AESEQ", IntegerType(), nullable=True),
    StructField("AETERM", StringType(), nullable=True),
    StructField("AESTDTC", TimestampType(), nullable=True),  # Unifying date format
    StructField("AESEV", StringType(), nullable=True),
    StructField("AEREL", StringType(), nullable=True),
    StructField("AEOUT", StringType(), nullable=True),
    StructField("AESTDY", FloatType(), nullable=True)  # Unified as Float
])

# Load the datasets from PostgreSQL
df1 = spark.read.jdbc(url=postgres_url, table="dataset_sdtm_ae1", properties=postgres_properties)
df2 = spark.read.jdbc(url=postgres_url, table="dataset_sdtm_ae2", properties=postgres_properties)
df3 = spark.read.jdbc(url=postgres_url, table="dataset_sdtm_ae3", properties=postgres_properties)

# Harmonize the column types
df1 = df1.withColumn("AESTDTC", col("AESTDTC").cast(TimestampType()))
df2 = df2.withColumn("AESTDTC", col("AESTDTC").cast(TimestampType()))
df3 = df3.withColumn("AESTDTC", col("AESTDTC").cast(TimestampType()))

df1 = df1.withColumn("AESTDY", col("AESTDY").cast(FloatType()))
df2 = df2.withColumn("AESTDY", col("AESTDY").cast(FloatType()))
df3 = df3.withColumn("AESTDY", col("AESTDY").cast(FloatType()))

# Select and align column names
df1 = df1.select("USUBJID", "AESEQ", "AETERM", "AESTDTC", "AESEV", "AEREL", "AEOUT", "AESTDY")
df2 = df2.select("USUBJID", "AESEQ", "AETERM", "AESTDTC", "AESEV", "AEREL", "AEOUT", "AESTDY")
df3 = df3.select("USUBJID", "AESEQ", "AETERM", "AESTDTC", "AESEV", "AEREL", "AEOUT", "AESTDY")

# Union the datasets together
unified_df = df1.unionByName(df2).unionByName(df3)

# Cast the DataFrame to match the unified schema
unified_df = unified_df.select(
    col("USUBJID").cast(StringType()),
    col("AESEQ").cast(IntegerType()),
    col("AETERM").cast(StringType()),
    col("AESTDTC").cast(TimestampType()),
    col("AESEV").cast(StringType()),
    col("AEREL").cast(StringType()),
    col("AEOUT").cast(StringType()),
    col("AESTDY").cast(FloatType())
)

# Write the unified dataset back to PostgreSQL
try:
    unified_df.write.jdbc(url=postgres_url, table="unified_sdtm_ae_dataset", mode="overwrite", properties=postgres_properties)
    print("Unified SDTM AE dataset written to PostgreSQL successfully.")
except Exception as e:
    print("Error while writing to PostgreSQL:", e)

# Stop the Spark session
spark.stop()
