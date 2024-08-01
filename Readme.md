<p align="center">
  <img src="https://raw.githubusercontent.com/PKief/vscode-material-icon-theme/ec559a9f6bfd399b82bb44393651661b08aaf7ba/icons/folder-markdown-open.svg" width="100" alt="project-logo">
</p>
<p align="center">
    <h1 align="center">BIGDATA</h1>
</p>
<p align="center">
    <em>Transforming game events into winning insights.</em>
</p>
<p align="center">
	<img src="https://img.shields.io/github/license/Cyb0nix/BigData?style=default&logo=opensourceinitiative&logoColor=white&color=0080ff" alt="license">
	<img src="https://img.shields.io/github/last-commit/Cyb0nix/BigData?style=default&logo=git&logoColor=white&color=0080ff" alt="last-commit">
	<img src="https://img.shields.io/github/languages/top/Cyb0nix/BigData?style=default&color=0080ff" alt="repo-top-language">
	<img src="https://img.shields.io/github/languages/count/Cyb0nix/BigData?style=default&color=0080ff" alt="repo-language-count">
<p>
<p align="center">
	<!-- default option, no dependency badges. -->
</p>

<br><!-- TABLE OF CONTENTS -->

<details>
  <summary>Table of Contents</summary><br>

- [ Overview](#-overview)
- [ Features](#-features)
- [ Repository Structure](#-repository-structure)
- [ Modules](#-modules)
- [ Getting Started](#-getting-started)
  - [ Installation](#-installation)
  - [ Usage](#-usage)
  </details>
  <hr>

## Overview

The BigData project, also known as HoopsAnalyzer, leverages Kafka, PostgreSQL, and Apache Spark technologies to simulate, process, and analyze basketball game events. This comprehensive system generates diverse game events, efficiently handles event consumption and storage, and performs advanced analytics to provide valuable insights for sports enthusiasts and analysts alike.

## Repository Structure

```sh
└── BigData/
    ├── Consumer.py
    ├── DataProcessor.py
    ├── Producer.py
    ├── Readme.md
    ├── database.sql
    ├── dbInit.py
    ├── docker-compose.yml
    ├── postgresql-42.7.3.jar
    └── requirements.txt
```

---

## Modules

<details closed><summary>.</summary>

| File                                                                                    | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| --------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Producer.py](https://github.com/Cyb0nix/BigData/blob/master/Producer.py)               | The Producer.py script generates random game events and sends them to the Kafka broker. The game events are generated using the Faker library. The script sends the game events to the Kafka broker using the KafkaProducer class from the kafka-python library and runs in an infinite loop and generates a new game event every second. The script also prints the game events to the console.                                               |
| [docker-compose.yml](https://github.com/Cyb0nix/BigData/blob/master/docker-compose.yml) | Establishes services for zookeeper, kafka, and postgres containers with specific configurations for image versions, ports, and environment variables. Facilitates efficient message processing and data storage within the BigData system.                                                                                                                                                                                                     |
| [dbInit.py](https://github.com/Cyb0nix/BigData/blob/master/dbInit.py)                   | Initializes PostgreSQL database with player, team, and game data, associating players to teams randomly. Handles data population and commits changes for sports analytics in the architecture.                                                                                                                                                                                                                                                 |
| [Consumer.py](https://github.com/Cyb0nix/BigData/blob/master/Consumer.py)               | The Consumer.py script consumes the game events from the Kafka broker. The script uses the KafkaConsumer class from the kafka-python library to consume the game events and stores them in a list called game_events. This list is stored in a postgres database using the psycopg2 library, each type of event is stored in a different table in the database. The script runs in an infinite loop and prints the game events to the console. |
| [DataProcessor.py](https://github.com/Cyb0nix/BigData/blob/master/DataProcessor.py)     | The DataProcessor.py script processes the game events using Spark and stores the results in a PostgreSQL database using the psycopg2 library and the Spark SQL library. The best player is calculated by counting the number of points scored by each player and selecting the player with the highest score of each game.                                                                                                                     |

</details>

---

## Getting Started

**System Requirements:**

- **Python**: `version x.y.z`

### Installation

<h4>From <code>source</code></h4>

> 1. Clone the BigData repository:
>
> ```console
> $ git clone https://github.com/Cyb0nix/BigData
> ```
>
> 2. Build and run Docker containers:
>
> ```console
> $ docker-compose up -d
> ```
>
> 3. Install the dependencies:
>
> ```console
> $ pip install -r requirements.txt
> ```
>
> 4. Initialize the database:
>
> ```console
> $ docker exec -i bigdata_postgres psql -U postgres -d postgres < database.sql
> $ python dbInit.py
> ```

### Usage

<h4>From <code>source</code></h4>

> 1. Start event generation:
>
> ```console
> $ python Producer.py
> ```
>
> 2. Start event consumption:
>
> ```console
> $ python Consumer.py
> ```
>
> 3. Start data processing:
>
> ```console
> $ python python DataProcessor.py
> ```

## Technologies Used

| Technology                                                                                                                | Description                             |
| ------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)                     | Main programming language               |
| ![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white) | Message broker for event streaming      |
| ![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white) | Data processing and analytics           |
| ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)         | Database for storing events and results |
| ![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)                     | Containerization and orchestration      |
| ![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=sql&logoColor=white)                              | Database operations                     |
