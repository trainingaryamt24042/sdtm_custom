from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder \
    .appName("Sum Ages") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://w3.training5.modak.com:5432/postgres"
table_name = "sdtm_dm"
output_filtered_table = "sdtm_dm_filtered"
output_sum_table = "sdtm_dm_age_sum"
properties = {"user": "mt24042", "password": "mt24042@m06y24", "driver": "org.postgresql.Driver"}

df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

filtered_df = df.where((df.age > 70) & (df.race == 'Asian'))

filtered_df.write.jdbc(url=jdbc_url, table=output_filtered_table, mode="overwrite", properties=properties)

age_sum = filtered_df.select(_sum(col("age")).alias("total_age"))

age_sum.write.jdbc(url=jdbc_url, table=output_sum_table, mode="overwrite", properties=properties)
spark.stop()
