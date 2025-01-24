from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sum Ages") \
    .config("spark.jars","/home/mt24042/Downloads/postgresql-42.7.5.jar") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://w3.training5.modak.com:5432/postgres"
table_name = "sdtm_dm"
properties = {"user": "mt24042", "password": "mt24042@m06y24", "driver": "org.postgresql.Driver"}

df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)


age_sum = df.select(_sum(col("age")).alias("total_age"))

age_sum.show()

spark.stop()
