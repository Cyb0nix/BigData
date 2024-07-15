from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("PadinET Data Processor")
         .config("spark.jars", "postgresql-42.7.3.jar")
         .getOrCreate()
         )


def read_data_from_postgres(table_name):
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df

def write_data_to_postgres(df):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "example_table") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

spark.stop()
