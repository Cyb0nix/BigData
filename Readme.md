# Basketball Game Event Generator

This project simulates a basketball game by generating random game events. It uses Python for the main logic, Kafka for the message broker and Spark for processing the game events. The game events are stored in a PostgreSQL database.

The project consists of three main components:
- Producer.py: Generates random game events and sends them to the Kafka broker.
- Consumer.py: Consumes the game events from the Kafka broker.
- DataProcessor.py: Processes the game events using Spark.

The project uses the following technologies:
- Docker: Used to run the Kafka broker and Spark.
- Kafka: Message broker used to send and receive game events.
- Spark: Used to process the game events.
- SQL: Used to store the game events.
- Python: Used to write the main logic.

## Code Explanation

### Producer.py

The Producer.py script generates random game events and sends them to the Kafka broker. 
The game events are generated using the Faker library. 
The script sends the game events to the Kafka broker using the KafkaProducer class from the kafka-python library and runs in an infinite loop and generates a new game event every second. 
The script also prints the game events to the console.

### Consumer.py

The Consumer.py script consumes the game events from the Kafka broker. 
The script uses the KafkaConsumer class from the kafka-python library to consume the game events and stores them in a list called game_events.
This list is stored in a postgres database using the psycopg2 library, each type of event is stored in a different table in the database.
The script runs in an infinite loop and prints the game events to the console.

### DataProcessor.py

The DataProcessor.py script processes the game events using Spark and stores the results in a PostgreSQL database using the psycopg2 library and the Spark SQL library.
The best player is calculated by counting the number of points scored by each player and selecting the player with the highest score of each game.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Python
- Docker
- Docker Compose

### Installation

1. Clone the repository
```bash
git clone https://github.com/Cyb0nix/BigData.git
```

2. Change directory
```bash
cd BigData
```

3. Build the Docker image and run the container
```bash
docker-compose up -d
```

4. Install the required Python libraries
```bash
pip install -r requirements.txt
```

5. Create the PostgreSQL database and tables by running the database.sql script
```bash
docker exec -i bigdata_postgres psql -U postgres -d postgres < database.sql
```
6. initialize the database by running the dbInit.py script
```bash
python dbInit.py
```

## Usage

Run the Producer.py script to start generating game events.

```bash
python Producer.py
```

Run the Consumer.py script to start consuming game events.

```bash
python Consumer.py
```

Run the DataProcessor.py script to start processing game events.

```bash
python DataProcessor.py
```

## Built With

- Python
- SQL
- Docker
- Kafka

