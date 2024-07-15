from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
        .option("dbtable", "player_statistics") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


# Read data from PostgreSQL
shot_event_df = read_data_from_postgres("shot_event")
pass_event_df = read_data_from_postgres("pass_event")
foul_event_df = read_data_from_postgres("foul_event")
rebound_event_df = read_data_from_postgres("rebound_event")
block_event_df = read_data_from_postgres("block_event")
steal_event_df = read_data_from_postgres("steal_event")
turnover_event_df = read_data_from_postgres("turnover_event")

# Calculate statistics for each DataFrame
shot_stats = shot_event_df.withColumn("madeShot", F.col("madeShot").cast("integer")) \
    .groupBy("playerId") \
    .agg(F.count("*").alias("total_shots"), F.sum("madeShot").alias("successful_shots"))
pass_stats = pass_event_df.groupBy("playerId").agg(F.count("*").alias("total_passes"), F.sum(
    F.when(F.col("passOutcome") == "completed", 1).otherwise(0)).alias("successful_passes"))
foul_stats = foul_event_df.groupBy("playerId").agg(F.count("*").alias("total_fouls"))
rebound_stats = rebound_event_df.groupBy("playerId").agg(F.count("*").alias("total_rebounds"))
block_stats = block_event_df.groupBy("playerId").agg(F.count("*").alias("total_blocks"))
steal_stats = steal_event_df.groupBy("playerId").agg(F.count("*").alias("total_steals"))
turnover_stats = turnover_event_df.groupBy("playerId").agg(F.count("*").alias("total_turnovers"))


# Union all DataFrames
player_stats = shot_stats.join(pass_stats, "playerId", "outer") \
    .join(foul_stats, "playerId", "outer") \
    .join(rebound_stats, "playerId", "outer") \
    .join(block_stats, "playerId", "outer") \
    .join(steal_stats, "playerId", "outer") \
    .join(turnover_stats, "playerId", "outer")

# Write data to PostgreSQL
write_data_to_postgres(player_stats)

spark.stop()
