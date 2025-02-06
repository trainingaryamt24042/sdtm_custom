from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType, TimestampType
import logging
import json
import os

logging.basicConfig(level=logging.INFO)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName('Data Unification Process') \
    .getOrCreate()

class DataUnifier:
    def __init__(self, schema_info_df, table_info_df, jdbc_url, db_properties):
        self.schema_info = schema_info_df.rdd.collectAsMap()
        self.table_info = table_info_df.rdd.collectAsMap()
        self.jdbc_url = jdbc_url
        self.db_properties = db_properties

    def get_dominant_type(self, type1, type2):
        priority = {"string": 1, "varchar": 1, "char": 1, "float": 2, "double": 3, "int": 4, "bigint": 5, "date": 6, "timestamp": 7}
        
        if "varchar" in type1 and "varchar" in type2:
            length1 = int(type1[type1.find('(')+1:type1.find(')')])
            length2 = int(type2[type2.find('(')+1:type2.find(')')])
            return f"varchar({max(length1, length2)})"
        
        return type1 if priority.get(type1, 0) > priority.get(type2, 0) else type2

    def build_schema(self, domain):
        schema = {}
        for column_metadata in self.schema_info.get(domain, []):
            col_name, col_type = column_metadata.split('|@|')
            schema[col_name] = self.get_dominant_type(schema.get(col_name, col_type), col_type)
        return schema

    def unify_data(self, unified_table, domain):
        try:
            schema = self.build_schema(domain)
            table_list = json.loads(self.table_info.get(domain, "{}"))

            unified_schema = StructType([
                StructField(col, StringType(), True) if dtype.startswith('varchar') or dtype == 'string' else
                StructField(col, FloatType(), True) if dtype in ('float', 'double') else
                StructField(col, IntegerType(), True) if 'int' in dtype else
                StructField(col, DateType(), True) if dtype == 'date' else
                StructField(col, TimestampType(), True) if dtype == 'timestamp' else
                StructField(col, StringType(), True)
                for col, dtype in schema.items()
            ] + [StructField("source_dataset", StringType(), True)])

            unified_df = spark.createDataFrame([], unified_schema)

            for table_name, col_info in table_list.items():
                logging.info(f"Processing table: {table_name} for domain: {domain}")
                temp_df = spark.read.jdbc(url=self.jdbc_url, table=table_name, properties=self.db_properties)
                temp_df = temp_df.selectExpr(*[f"CAST({col} AS {col_info.get(col, 'string')}) AS {col}" for col in schema], f"'{table_name}' AS source_dataset")
                unified_df = unified_df.unionByName(temp_df)

            unified_df.write.jdbc(url=self.jdbc_url, table=unified_table, mode="overwrite", properties=self.db_properties)
            logging.info(f"Unified data for domain '{domain}' saved to {unified_table} in PostgreSQL")
        except Exception as e:
            logging.error(f"Unification failed for domain '{domain}': {e}")


def main():
    jdbc_url = "jdbc:postgresql://w3.training5.modak.com:5432/postgres"
    db_properties = {"user": "MT24042", "password": "mt24042@m06y24", "driver": "org.postgresql.Driver"}
    
    schema_info_df = spark.createDataFrame([
        ("dm", "USUBJID|@|varchar(30)"),
        ("dm", "AGE|@|float"),
        ("dm", "SEX|@|char(1)"),
        ("dm", "ARM|@|varchar(50)")
    ], ["domain", "column_meta"])

    table_info_df = spark.createDataFrame([
        ("dm", json.dumps({
            "dataset_sdtm1": {"USUBJID": "varchar(20)", "AGE": "int", "SEX": "char(1)"},
            "dataset_sdtm2": {"USUBJID": "varchar(20)", "AGE": "float", "SEX": "char(1)", "ARM": "varchar(50)"},
            "dataset_sdtm3": {"USUBJID": "varchar(30)", "AGE": "float", "SEX": "char(1)", "ARM": "varchar(50)"}
        }))
    ], ["domain", "tbl_meta"])

    unifier = DataUnifier(schema_info_df, table_info_df, jdbc_url, db_properties)
    unifier.unify_data("unified_dataset", "dm")

if __name__ == "__main__":
    main()
