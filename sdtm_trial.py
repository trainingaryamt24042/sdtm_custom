from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, expr, avg, datediff

spark = SparkSession.builder \
    .appName("Sum Ages") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://w3.training5.modak.com:5432/postgres"
table_name = "sdtm_dm"
output_filtered_table = "sdtm_dm_filtered"
output_sum_table = "sdtm_dm_age_sum"
output_sum_avg = "sdtm_avg_age_race"
date_diff_table = "sdtm_date_diff_table"
properties = {"user": "mt24042", "password": "mt24042@m06y24", "driver": "org.postgresql.Driver"}

def skew_birthdate(df, column_name):
    return df.withColumn(column_name, expr(f"date_add({column_name}, 5)"))

def avg_age_per_race(df, column_name):
    return df.groupBy("race").agg(avg("age").alias("avg_age"))

df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

avg_age_df = avg_age_per_race(df, "race")
avg_age_df.write.jdbc(url=jdbc_url, table=output_sum_avg, mode="overwrite", properties=properties)

df_birthdate = skew_birthdate(df, "brthdt")
df_birthdate.write.jdbc(url=jdbc_url, table=output_filtered_table, mode="overwrite", properties=properties)

filtered_df = df_birthdate.where((df_birthdate.age > 70) & (df_birthdate.race == 'Asian'))
filtered_df.write.jdbc(url=jdbc_url, table=output_filtered_table, mode="overwrite", properties=properties)

date_diff = df.withColumn("date_diff", datediff(df["rfendtc"], df["rfstdtc"]))
date_diff.write.jdbc(url=jdbc_url, table=date_diff_table, mode="overwrite", properties=properties)

age_sum = filtered_df.select(_sum(col("age")).alias("total_age"))
age_sum.write.jdbc(url=jdbc_url, table=output_sum_table, mode="overwrite", properties=properties)

spark.stop()
