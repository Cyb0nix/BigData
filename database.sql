-- Game Information
CREATE TABLE game (
    gameId INT PRIMARY KEY,
    -- Add other game details here (e.g., startTime, location, etc.)
);

-- Teams
CREATE TABLE team (
    teamId INT PRIMARY KEY AUTO_INCREMENT,
    teamName VARCHAR(255)
);

-- Players
CREATE TABLE player (
    playerId INT PRIMARY KEY,
    playerName VARCHAR(255),
    teamId INT,
    FOREIGN KEY (teamId) REFERENCES team(teamId)
);

-- Main Game Events
CREATE TABLE game_event (
    eventId BIGINT PRIMARY KEY AUTO_INCREMENT, 
    timestamp BIGINT,
    gameId INT,
    eventType VARCHAR(50),
    teamId INT,
    playerId INT,
    FOREIGN KEY (gameId) REFERENCES game(gameId),
    FOREIGN KEY (teamId) REFERENCES team(teamId),
    FOREIGN KEY (playerId) REFERENCES player(playerId)
);

-- Event-Specific Tables
CREATE TABLE shot_event (
    eventId BIGINT PRIMARY KEY,
    shotType VARCHAR(10),
    madeShot BOOLEAN,
    points INT,
    assistPlayerId INT,
    shotClock INT,
    shotDistance DECIMAL(5, 2),
    FOREIGN KEY (eventId) REFERENCES game_event(eventId),
    FOREIGN KEY (assistPlayerId) REFERENCES player(playerId)  -- Link to player if assisted
);

CREATE TABLE pass_event (
    eventId BIGINT PRIMARY KEY,
    passType VARCHAR(20),
    receiverPlayerId INT,
    passOutcome VARCHAR(20),
    passDistance DECIMAL(5, 2),
    FOREIGN KEY (eventId) REFERENCES game_event(eventId),
    FOREIGN KEY (receiverPlayerId) REFERENCES player(playerId)
);

CREATE TABLE foul_event (
    eventId BIGINT PRIMARY KEY,
    foulType VARCHAR(20),
    fouledPlayerId INT,
    FOREIGN KEY (eventId) REFERENCES game_event(eventId),
    FOREIGN KEY (fouledPlayerId) REFERENCES player(playerId)
);

CREATE TABLE rebound_event (
    eventId BIGINT PRIMARY KEY,
    reboundType VARCHAR(10),
    FOREIGN KEY (eventId) REFERENCES game_event(eventId)
);

CREATE TABLE block_event (
    eventId BIGINT PRIMARY KEY,
    blockedPlayerId INT,
    FOREIGN KEY (eventId) REFERENCES game_event(eventId),
    FOREIGN KEY (blockedPlayerId) REFERENCES player(playerId)
);

CREATE TABLE steal_event (
    eventId BIGINT PRIMARY KEY,
    stolenFromPlayerId INT,
    FOREIGN KEY (eventId) REFERENCES game_event(eventId),
    FOREIGN KEY (stolenFromPlayerId) REFERENCES player(playerId)
);

CREATE TABLE turnover_event (
    eventId BIGINT PRIMARY KEY,
    turnoverType VARCHAR(20),
    FOREIGN KEY (eventId) REFERENCES game_event(eventId)
);

CREATE TABLE substitution_event (
    eventId BIGINT PRIMARY KEY,
    enteringPlayerId INT,
    leavingPlayerId INT,
    FOREIGN KEY (eventId) REFERENCES game_event(eventId),
    FOREIGN KEY (enteringPlayerId) REFERENCES player(playerId),
    FOREIGN KEY (leavingPlayerId) REFERENCES player(playerId)
);

CREATE TABLE timeout_event (
    eventId BIGINT PRIMARY KEY,
    timeoutType VARCHAR(10),
    FOREIGN KEY (eventId) REFERENCES game_event(eventId)
);

CREATE TABLE jump_ball_event (
    eventId BIGINT PRIMARY KEY,
    jumpBallWinner INT,
    losingTeam INT,
    FOREIGN KEY (eventId) REFERENCES game_event(eventId),
    FOREIGN KEY (jumpBallWinner) REFERENCES team(teamId),
    FOREIGN KEY (losingTeam) REFERENCES team(teamId)
);

CREATE TABLE player_statistics (
    stat_id SERIAL PRIMARY KEY,
    player_id INT,
    game_id INT,
    timestamp BIGINT,
    points INT DEFAULT 0,
    assists INT DEFAULT 0,
    rebounds INT DEFAULT 0,
    steals INT DEFAULT 0,
    blocks INT DEFAULT 0,
    turnovers INT DEFAULT 0,
    fouls INT DEFAULT 0,
    minutes_played INT DEFAULT 0,
    field_goals_made INT DEFAULT 0,
    field_goals_attempted INT DEFAULT 0,
    three_pointers_made INT DEFAULT 0,
    three_pointers_attempted INT DEFAULT 0,
    free_throws_made INT DEFAULT 0,
    free_throws_attempted INT DEFAULT 0,
    plus_minus INT DEFAULT 0,
    FOREIGN KEY (player_id) REFERENCES player(playerId),
    FOREIGN KEY (game_id) REFERENCES game(gameId)
);