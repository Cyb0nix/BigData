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
        
        
    
shot_event_df = read_data_from_postgres("shot_event")
pass_event_df = read_data_from_postgres("pass_event")
foul_event_df = read_data_from_postgres("foul_event")
rebound_event_df = read_data_from_postgres("rebound_event")
block_event_df = read_data_from_postgres("block_event")
steal_event_df = read_data_from_postgres("steal_event")
turnover_event_df = read_data_from_postgres("turnover_event")

# Calculate statistics for each DataFrame
shot_stats = shot_event_df.withColumn("madeShot", F.col("madeShot").cast("integer")) \
    .groupBy("playerId", "gameId") \
    .agg(
        F.count("*").alias("field_goals_attempted"),
        F.sum("madeShot").alias("field_goals_made"),
        F.sum(F.when(F.col("shotType") == "3PT", 1).otherwise(0)).alias("three_pointers_attempted"),
        F.sum(F.when((F.col("shotType") == "3PT") & (F.col("madeShot") == 1), 1).otherwise(0)).alias("three_pointers_made"),
        F.sum(F.when(F.col("shotType") == "FT", 1).otherwise(0)).alias("free_throws_attempted"),
        F.sum(F.when((F.col("shotType") == "FT") & (F.col("madeShot") == 1), 1).otherwise(0)).alias("free_throws_made")
    )

pass_stats = pass_event_df.groupBy("playerId", "gameId") \
    .agg(
        F.count("*").alias("total_passes"),
        F.sum(F.when(F.col("passOutcome") == "completed", 1).otherwise(0)).alias("successful_passes")
    )

foul_stats = foul_event_df.groupBy("playerId", "gameId").agg(F.count("*").alias("fouls"))
rebound_stats = rebound_event_df.groupBy("playerId", "gameId").agg(F.count("*").alias("rebounds"))
block_stats = block_event_df.groupBy("playerId", "gameId").agg(F.count("*").alias("blocks"))
steal_stats = steal_event_df.groupBy("playerId", "gameId").agg(F.count("*").alias("steals"))
turnover_stats = turnover_event_df.groupBy("playerId", "gameId").agg(F.count("*").alias("turnovers"))

# Union all DataFrames
player_stats = shot_stats.join(pass_stats, ["playerId", "gameId"], "outer") \
    .join(foul_stats, ["playerId", "gameId"], "outer") \
    .join(rebound_stats, ["playerId", "gameId"], "outer") \
    .join(block_stats, ["playerId", "gameId"], "outer") \
    .join(steal_stats, ["playerId", "gameId"], "outer") \
    .join(turnover_stats, ["playerId", "gameId"], "outer")

# Calculate percentages and additional statistics
player_stats = player_stats.withColumn("field_goal_percentage", F.round(F.col("field_goals_made") / F.col("field_goals_attempted") * 100, 2)) \
    .withColumn("three_point_percentage", F.round(F.col("three_pointers_made") / F.col("three_pointers_attempted") * 100, 2)) \
    .withColumn("free_throw_percentage", F.round(F.col("free_throws_made") / F.col("free_throws_attempted") * 100, 2)) \
    .withColumn("points", F.col("field_goals_made") * 2 + F.col("three_pointers_made") + F.col("free_throws_made")) \
    .withColumn("effective_field_goal_percentage", F.round((F.col("field_goals_made") + 0.5 * F.col("three_pointers_made")) / F.col("field_goals_attempted") * 100, 2)) \
    .withColumn("true_shooting_percentage", F.round(F.col("points") / (2 * (F.col("field_goals_attempted") + 0.44 * F.col("free_throws_attempted"))) * 100, 2)) \
    .withColumn("assist_percentage", F.round(F.col("successful_passes") / F.col("total_passes") * 100, 2))

# Add timestamp
player_stats = player_stats.withColumn("timestamp", F.current_timestamp())

# Write data to PostgreSQL
write_data_to_postgres(player_stats)

spark.stop()