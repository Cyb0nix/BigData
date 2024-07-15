from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

# 1. Count of events by type
event_counts = game_event_df.groupBy("eventType").count().orderBy(F.desc("count"))
game_event_df = read_data_from_postgres("game_event")
shot_event_df = read_data_from_postgres("shot_event")
pass_event_df = read_data_from_postgres("pass_event")
foul_event_df = read_data_from_postgres("foul_event")
rebound_event_df = read_data_from_postgres("rebound_event")
block_event_df = read_data_from_postgres("block_event")
steal_event_df = read_data_from_postgres("steal_event")
turnover_event_df = read_data_from_postgres("turnover_event")
player_df = read_data_from_postgres("player")
team_df = read_data_from_postgres("team")
    

# 2. Player scoring statistics
player_scoring = game_event_df.join(shot_event_df, "eventId") \
    .join(player_df, game_event_df.playerId == player_df.playerId) \
    .groupBy("playerName") \
    .agg(
        F.sum("points").alias("total_points"),
        F.avg("points").alias("avg_points_per_shot"),
        F.sum(F.when(F.col("madeShot") == True, 1).otherwise(0)).alias("shots_made"),
        F.count("*").alias("total_shots")
    ) \
    .withColumn("shooting_percentage", F.col("shots_made") / F.col("total_shots") * 100) \
    .orderBy(F.desc("total_points"))

# 3. Team statistics
team_stats = game_event_df.join(team_df, game_event_df.teamId == team_df.teamId) \
    .join(shot_event_df, "eventId", "left") \
    .groupBy("teamName") \
    .agg(
        F.count(F.when(F.col("eventType") == "shot", 1)).alias("total_shots"),
        F.sum(F.when(F.col("eventType") == "shot", F.col("points"))).alias("total_points"),
        F.count(F.when(F.col("eventType") == "rebound", 1)).alias("total_rebounds"),
        F.count(F.when(F.col("eventType") == "turnover", 1)).alias("total_turnovers"),
        F.count(F.when(F.col("eventType") == "foul", 1)).alias("total_fouls")
    )

# 4. Most frequent event types
frequent_events = game_event_df.groupBy("eventType") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(5)

# 5. Player with most assists
top_assisters = game_event_df.join(shot_event_df, "eventId") \
    .filter(shot_event_df.assistPlayerId.isNotNull()) \
    .join(player_df, shot_event_df.assistPlayerId == player_df.playerId) \
    .groupBy("playerName") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(5)

# 6. Shot clock analysis
shot_clock_analysis = game_event_df.join(shot_event_df, "eventId") \
    .groupBy("shotClock") \
    .agg(
        F.count("*").alias("total_shots"),
        F.avg("points").alias("avg_points")
    ) \
    .orderBy("shotClock")

# 7. Shot distance analysis
shot_distance_bins = game_event_df.join(shot_event_df, "eventId") \
    .withColumn("distance_bin", F.floor(shot_event_df.shotDistance / 5) * 5) \
    .groupBy("distance_bin") \
    .agg(
        F.count("*").alias("total_shots"),
        F.avg("points").alias("avg_points"),
        F.sum(F.when(F.col("madeShot") == True, 1).otherwise(0)).alias("shots_made")
    ) \
    .withColumn("shooting_percentage", F.col("shots_made") / F.col("total_shots") * 100) \
    .orderBy("distance_bin")

# 8. Player performance over time
player_performance = game_event_df.join(shot_event_df, "eventId") \
    .join(player_df, game_event_df.playerId == player_df.playerId) \
    .withColumn("minute", F.floor(F.col("timestamp") / 60000)) \
    .groupBy("playerName", "minute") \
    .agg(F.sum("points").alias("points")) \
    .orderBy("playerName", "minute")

# 9. Foul analysis
foul_analysis = game_event_df.join(foul_event_df, "eventId") \
    .join(team_df, game_event_df.teamId == team_df.teamId) \
    .groupBy("teamName", "foulType") \
    .count() \
    .orderBy("teamName", F.desc("count"))

# 10. Turnover analysis
turnover_analysis = game_event_df.join(turnover_event_df, "eventId") \
    .join(team_df, game_event_df.teamId == team_df.teamId) \
    .groupBy("teamName", "turnoverType") \
    .count() \
    .orderBy("teamName", F.desc("count"))

spark.stop()
