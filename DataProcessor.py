from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# Initialize Spark session
spark = (SparkSession.builder
         .appName("PadinET Data Processor")
         .config("spark.jars", "postgresql-42.7.3.jar")
         .getOrCreate())


def read_data_from_postgres(table_name):
    return spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()


def write_data_to_postgres(df):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "player_statistics") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()


# Read data from PostgreSQL
game_event_df = read_data_from_postgres("game_event")
shot_event_df = read_data_from_postgres("shot_event")
pass_event_df = read_data_from_postgres("pass_event")
foul_event_df = read_data_from_postgres("foul_event")
rebound_event_df = read_data_from_postgres("rebound_event")
block_event_df = read_data_from_postgres("block_event")
steal_event_df = read_data_from_postgres("steal_event")
turnover_event_df = read_data_from_postgres("turnover_event")

# Join data
game_event_df = game_event_df.alias("game_event")
shot_event_df = shot_event_df.alias("shot_event")
pass_event_df = pass_event_df.alias("pass_event")
foul_event_df = foul_event_df.alias("foul_event")
rebound_event_df = rebound_event_df.alias("rebound_event")
block_event_df = block_event_df.alias("block_event")
steal_event_df = steal_event_df.alias("steal_event")
turnover_event_df = turnover_event_df.alias("turnover_event")

# Join game_event with specific event tables
game_event_df = game_event_df.join(shot_event_df, game_event_df.eventid == shot_event_df.eventid, "left") \
    .join(pass_event_df, game_event_df.eventid == pass_event_df.eventid, "left") \
    .join(foul_event_df, game_event_df.eventid == foul_event_df.eventid, "left") \
    .join(rebound_event_df, game_event_df.eventid == rebound_event_df.eventid, "left") \
    .join(block_event_df, game_event_df.eventid == block_event_df.eventid, "left") \
    .join(steal_event_df, game_event_df.eventid == steal_event_df.eventid, "left") \
    .join(turnover_event_df, game_event_df.eventid == turnover_event_df.eventid, "left")

# Calculate statistics
player_stats = game_event_df.groupBy("game_event.playerId", "game_event.gameId").agg(
    F.sum(F.when(game_event_df.eventtype == "shot", 1).otherwise(0)).alias("field_goals_attempted"),
    F.sum(F.when((game_event_df.eventtype == "shot") & (shot_event_df.madeshot == True), 1).otherwise(0)).alias(
        "field_goals_made"),
    F.sum(F.when((game_event_df.eventtype == "shot") & (shot_event_df.shottype == "3pt"), 1).otherwise(0)).alias(
        "three_pointers_attempted"),
    F.sum(F.when(
        (game_event_df.eventtype == "shot") & (shot_event_df.shottype == "3pt") & (shot_event_df.madeshot == True),
        1).otherwise(0)).alias("three_pointers_made"),
    F.sum(F.when((game_event_df.eventtype == "shot") & (shot_event_df.shottype == "freethrow"), 1).otherwise(0)).alias(
        "free_throws_attempted"),
    F.sum(F.when((game_event_df.eventtype == "shot") & (shot_event_df.shottype == "freethrow") & (
            shot_event_df.madeshot == True), 1).otherwise(0)).alias("free_throws_made"),
    F.sum(F.when(game_event_df.eventtype == "pass", 1).otherwise(0)).alias("total_passes"),
    F.sum(
        F.when((game_event_df.eventtype == "pass") & (pass_event_df.passoutcome == "completed"), 1).otherwise(0)).alias(
        "successful_passes"),
    F.sum(F.when(game_event_df.eventtype == "foul", 1).otherwise(0)).alias("fouls"),
    F.sum(F.when(game_event_df.eventtype == "rebound", 1).otherwise(0)).alias("rebounds"),
    F.sum(F.when(game_event_df.eventtype == "block", 1).otherwise(0)).alias("blocks"),
    F.sum(F.when(game_event_df.eventtype == "steal", 1).otherwise(0)).alias("steals"),
    F.sum(F.when(game_event_df.eventtype == "turnover", 1).otherwise(0)).alias("turnovers"),
    ((F.sum(F.when((game_event_df.eventtype == "shot") & (shot_event_df.madeshot == True), 1).otherwise(0)) * 2) +
     (F.sum(F.when((game_event_df.eventtype == "shot") & (shot_event_df.shottype == "three-pointer") & (
             shot_event_df.madeshot == True), 1).otherwise(0)) * 3) +
     F.sum(F.when((game_event_df.eventtype == "shot") & (shot_event_df.shottype == "free-throw") & (
             shot_event_df.madeshot == True), 1).otherwise(0))).alias("points")
)

# Write data to PostgreSQL
write_data_to_postgres(player_stats)

# Print the best player in each game based on points
print("Best player in each game:")
window = Window.partitionBy("gameId")
player_stats = player_stats.withColumn("max_points", F.max("points").over(window))
player_stats = player_stats.filter(player_stats.points == player_stats.max_points)
player_stats = player_stats.drop("max_points")
player_stats.show()

# Stop Spark session
spark.stop()
