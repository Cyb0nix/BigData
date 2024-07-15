# consume kafka messages and store them in postgres

from database import DatabaseHandler
from kafka import KafkaConsumer

database_handler = DatabaseHandler(
    dbname="PadinET",
    user="postgres",
    password="potgres"
)

consumer = KafkaConsumer(
    "PadinET",
    bootstrap_servers="localhost:9092"
)

for message in consumer:
    database_handler.execute_query(
        "INSERT INTO example_table (message) VALUES (%s)",
        (message.value,)
    )

consumer.close()
database_handler.close()
