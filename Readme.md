 Basketball Game Event Generator

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

