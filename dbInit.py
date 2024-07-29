import psycopg2
import random

# Player and team data
players = {
    1: 'LeBron James',
    2: 'Stephen Curry',
    3: 'Kevin Durant',
    4: 'Kawhi Leonard',
    5: 'James Harden',
    6: 'Anthony Davis',
    7: 'Luka Doncic',
    8: 'Giannis Antetokounmpo',
    9: 'Damian Lillard',
    10: 'Nikola Jokic',
    11: 'Jayson Tatum',
    12: 'Jimmy Butler',
    13: 'Paul George',
    14: 'Donovan Mitchell',
    15: 'Karl-Anthony Towns',
    16: 'Zion Williamson',
    17: 'Devin Booker',
    18: 'Trae Young',
    19: 'Joel Embiid',
    20: 'Bradley Beal',
}
teams = [1, 2] # Assuming team IDs are 1 and 2

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
cur = conn.cursor()

# populate the team table
for team_id in teams:
    cur.execute(
        "INSERT INTO team (teamId, teamName) VALUES (%s, %s)",
        (team_id, 'Home Team' if team_id == 1 else 'Away Team')
    )

# Populate the player table
for player_id, player_name in players.items():
    cur.execute(
        "INSERT INTO player (playerId, playerName) VALUES (%s, %s)",
        (player_id, player_name)
    )

# Associate each player to a team randomly
player_team = {player_id: random.choice(teams) for player_id in players.keys()}

# Update the player table with the team associations
for player_id, team_id in player_team.items():
    cur.execute(
        "UPDATE player SET teamId = %s WHERE playerId = %s",
        (team_id, player_id)
    )

# populate the game table
for game_id in range(1, 3):
    cur.execute(
        "INSERT INTO game (gameId) VALUES (%s)",
        (game_id,)
    )

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()
