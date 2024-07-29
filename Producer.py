from kafka import KafkaProducer
import json
import time
import random

# Kafka Configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'basketball_game_events'

# Producer Setup
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=5,
)

# Game Information (Replace with actual data)
game_id = [1, 2]
teams = ['Home Team', 'Away Team']
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

# Event Types and Details
event_types = ['shot', 'pass', 'foul', 'rebound', 'block', 'steal', 'turnover', 'substitution', 'timeout', 'jump ball']
shot_types = ['2pt', '3pt', 'freethrow']
foul_types = ['personal', 'technical', 'flagrant']
turnover_types = ['bad pass', 'traveling', 'offensive foul', 'double dribble', 'out of bounds']
rebound_types = ['offensive', 'defensive']


def generate_game_event():
    event_type = random.choice(event_types)
    team = random.choice(teams)
    player_id = random.randint(1, len(players))
    player_name = players.get(player_id)

    event = {
        'timestamp': int(time.time() * 1000),
        'gameId': random.choice(game_id),
        'eventType': event_type,
        'team': team,
        'playerId': player_id,
        'playerName': player_name,
    }

    # Event-Specific Details
    if event_type == 'shot':
        event['shotType'] = random.choice(shot_types)
        event['madeShot'] = random.random() < 0.5
        event['points'] = 3 if event['shotType'] == '3pt' and event['madeShot'] else (2 if event['madeShot'] else 0)
        event['assistPlayerId'] = random.randint(1, len(players)) if random.random() < 0.7 and event[
            'madeShot'] else None
        event['shotClock'] = random.randint(0, 24)  # Add shot clock
        event['shotDistance'] = round(random.uniform(0, 30), 1)  # Add shot distance
    elif event_type == 'pass':
        event['passType'] = random.choice(['chest', 'bounce', 'overhead', 'behind the back'])
        event['receiverPlayerId'] = random.randint(1, len(players))
        event['passOutcome'] = random.choice(['completed', 'intercepted'])
        event['passDistance'] = round(random.uniform(0, 50), 1)
    elif event_type == 'foul':
        event['foulType'] = random.choice(foul_types)
        event['fouledPlayerId'] = random.randint(1, len(players))
    elif event_type == 'rebound':
        event['reboundType'] = random.choice(rebound_types)
    elif event_type == 'block':
        event['blockedPlayerId'] = random.randint(1, len(players))
    elif event_type == 'steal':
        event['stolenFromPlayerId'] = random.randint(1, len(players))
    elif event_type == 'turnover':
        event['turnoverType'] = random.choice(turnover_types)
    elif event_type == 'substitution':
        event['enteringPlayerId'] = random.randint(1, len(players))
        event['leavingPlayerId'] = random.randint(1, len(players))
    elif event_type == 'timeout':
        event['timeoutType'] = random.choice(['full', '20sec'])
    elif event_type == 'jump ball':
        event['jumpBallWinner'] = random.choice(teams)
        event['losingTeam'] = teams[0] if event['jumpBallWinner'] == teams[1] else teams
    else:
        pass

    return event


# Main Event Loop
while True:
    event = generate_game_event()
    print(f"Sending event: {event}")
    producer.send(topic_name, value=event)
