from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sum Ages") \
    .config("spark.jars", "/home/mt24042/Downloads/postgresql-42.7.5.jar") \
    .getOrCreate()

# JDBC connection properties
jdbc_url = "jdbc:postgresql://w3.training5.modak.com:5432/postgres"
table_name = "sdtm_dm"
output_filtered_table = "sdtm_dm_filtered"
output_sum_table = "sdtm_dm_age_sum"
properties = {"user": "mt24042", "password": "mt24042@m06y24", "driver": "org.postgresql.Driver"}

# Read the table from the database
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# Apply filters
filtered_df = df.where((df.age > 70) & (df.race == 'Asian'))

# Write the filtered data to a new table
filtered_df.write.jdbc(url=jdbc_url, table=output_filtered_table, mode="overwrite", properties=properties)

# Calculate the sum of ages
age_sum = filtered_df.select(_sum(col("age")).alias("total_age"))

# Write the sum of ages to another table
age_sum.write.jdbc(url=jdbc_url, table=output_sum_table, mode="overwrite", properties=properties)

# Stop the Spark session
spark.stop()
