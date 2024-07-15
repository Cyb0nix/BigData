from kafka import KafkaConsumer
import json
import psycopg2

# Kafka Configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'basketball_game_events'

# PostgreSQL Configuration
pg_host = 'localhost'
pg_port = 5432
pg_database = 'postgres'
pg_user = 'postgres'
pg_password = 'postgres'

# Consumer Setup
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='basketball_game_events_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# PostgreSQL Connection
conn = psycopg2.connect(
    host=pg_host,
    port=pg_port,
    database=pg_database,
    user=pg_user,
    password=pg_password
)
cur = conn.cursor()

team_id_map = {
    'Home Team': 1,
    'Away Team': 2,
}

# Dictionary for Mapping Event Types to Tables
event_table_map = {
    'shot': 'shot_event',
    'pass': 'pass_event',
    'foul': 'foul_event',
    'rebound': 'rebound_event',
    'block': 'block_event',
    'steal': 'steal_event',
    'turnover': 'turnover_event',
    'substitution': 'substitution_event',
    'timeout': 'timeout_event',
    'jump ball': 'jump_ball_event',
}

# Dictionary for Mapping Event Types to Columns
event_column_map = {
    'shot': ['playerId', 'shotType', 'madeShot', 'points', 'assistPlayerId', 'shotClock', 'shotDistance'],
    'pass': ['playerId', 'passType', 'receiverPlayerId', 'passOutcome', 'passDistance'],
    'foul': ['playerId', 'foulType', 'fouledPlayerId'],
    'rebound': ['playerId', 'reboundType'],
    'block': ['playerId', 'blockedPlayerId'],
    'steal': ['playerId', 'stolenFromPlayerId'],
    'turnover': ['playerId', 'turnoverType'],
    'substitution': ['enteringPlayerId', 'leavingPlayerId'],
    'timeout': ['playerId', 'timeoutType'],
    'jump ball': ['jumpBallWinner', 'losingTeam']
}

def insert_game_event(game_event):
    # First, insert into the main `game_event` table
    cur.execute(
        "INSERT INTO game_event (timestamp, gameId, eventType, teamId, playerId) VALUES (%s, %s, %s, %s, %s) RETURNING eventId",
        (game_event['timestamp'], game_event['gameId'], game_event['eventType'], team_id_map.get(game_event['team']),
         game_event['playerId']))
    event_id = cur.fetchone()[0]  # Get the generated eventId

    # Then, insert into the specific event table based on eventType
    event_table = event_table_map.get(game_event['eventType'])
    event_columns = event_column_map.get(game_event['eventType'])
    event_values = [game_event.get(column) for column in event_columns]

    # Convert team names to their respective IDs
    for column in ['team', 'jumpBallWinner', 'losingTeam']:
        if column in event_columns:
            index = event_columns.index(column)
            if isinstance(event_values[index], list):
                event_values[index] = team_id_map.get(event_values[index][0])  # Take the first element of the list
            else:
                event_values[index] = team_id_map.get(event_values[index])

    event_values.insert(0, event_id)  # Insert eventId as the first column
    event_values = tuple(event_values)
    cur.execute(
        f"INSERT INTO {event_table} (eventId, {', '.join(event_columns)}) VALUES ({', '.join(['%s'] * (len(event_columns) + 1))})",
        event_values)
    conn.commit()


# Main Consumer Loop
for message in consumer:
    print(f"Received message: {message}")
    event = message.value
    print(f"Received event: {event}")
    insert_game_event(event)
    print(f"Inserted event: {event}")