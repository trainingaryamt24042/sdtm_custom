from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from urllib.parse import quote_plus

spark = SparkSession.builder \
    .appName("Unify Datasets") \
    .config("spark.jars", "/home/mt24042/Downloads/postgresql-42.7.5.jar") \
    .getOrCreate()


# Define the PostgreSQL connection details
postgres_url = "jdbc:postgresql://w3.training5.modak.com:5432/postgres"  # Use the correct host and port
postgres_properties = {
    "user": "mt24042",
    "password": "mt24042@m06y24",
    "driver": "org.postgresql.Driver"
}

# Define the schema of the unified table
unified_schema = StructType([
    StructField("USUBJID", StringType(), nullable=False),
    StructField("SEX", StringType(), nullable=True),  # Using StringType for better compatibility
    StructField("AGE", FloatType(), nullable=True),
    StructField("AGEU", StringType(), nullable=True),
    StructField("DTHDT", DateType(), nullable=True),
    StructField("DTHFL", StringType(), nullable=True),  # Using StringType for DTHFL
    StructField("ARM", StringType(), nullable=True),
    StructField("ARMCD", StringType(), nullable=True),
    StructField("SITEID", IntegerType(), nullable=True)
])

# Load the datasets from PostgreSQL
df1 = spark.read.jdbc(url=postgres_url, table="dataset_sdtm1", properties=postgres_properties)
df2 = spark.read.jdbc(url=postgres_url, table="dataset_sdtm2", properties=postgres_properties)
df3 = spark.read.jdbc(url=postgres_url, table="dataset_sdtm3", properties=postgres_properties)

# Harmonize the column types for AGE
df1 = df1.withColumn("AGE", col("AGE").cast(FloatType()))
df2 = df2.withColumn("AGE", col("AGE").cast(FloatType()))
df3 = df3.withColumn("AGE", col("AGE").cast(FloatType()))

# Ensure consistent column names across all datasets
df1 = df1.select("USUBJID", "SEX", "AGE", "AGEU", "DTHDT", "DTHFL", "ARM", "ARMCD", "SITEID")
df2 = df2.select("USUBJID", "SEX", "AGE", "AGEU", "DTHDT", "DTHFL", "ARM", "ARMCD", "SITEID")
df3 = df3.select("USUBJID", "SEX", "AGE", "AGEU", "DTHDT", "DTHFL", "ARM", "ARMCD", "SITEID")

# Union the datasets together
unified_df = df1.unionByName(df2).unionByName(df3)

# Cast the DataFrame to match the defined schema
unified_df = unified_df.select(
    col("USUBJID").cast(StringType()),
    col("SEX").cast(StringType()),  # Changed to StringType
    col("AGE").cast(FloatType()),
    col("AGEU").cast(StringType()),
    col("DTHDT").cast(DateType()),
    col("DTHFL").cast(StringType()),  # Changed to StringType
    col("ARM").cast(StringType()),
    col("ARMCD").cast(StringType()),
    col("SITEID").cast(IntegerType())
)

# Now let's write the unified DataFrame back to PostgreSQL
try:
    unified_df.write.jdbc(url=postgres_url, table="unified_sdtm_dataset", mode="overwrite", properties=postgres_properties)
    print("Data unified and written to PostgreSQL successfully.")
except Exception as e:
    print("Error while writing to PostgreSQL:", e)

# Stop the Spark session
spark.stop()
