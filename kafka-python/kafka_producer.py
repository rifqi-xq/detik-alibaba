from confluent_kafka import Consumer, KafkaException, KafkaError
import logging
import setting

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Kafka configuration
conf = setting.kafka_setting
consumer = Consumer(
    {
        "bootstrap.servers": conf["bootstrap_servers"],
        "sasl.mechanisms": "PLAIN",
        "ssl.ca.location": conf["ca_location"],
        "security.protocol": "SASL_SSL",
        "ssl.endpoint.identification.algorithm": "none",
        "sasl.username": conf["sasl_plain_username"],
        "sasl.password": conf["sasl_plain_password"],
        "group.id": "data-streaming-consumer",  # Unique group ID for consumer
        "auto.offset.reset": "earliest",  # Start from the earliest message
    }
)
TOPIC_NAME = conf["topic_name"]


def consume_from_kafka():
    try:
        consumer.subscribe([TOPIC_NAME])
        logging.info(f"Subscribed to Kafka topic: {TOPIC_NAME}")

        while True:
            msg = consumer.poll(timeout=1.0)  # Poll messages with a timeout
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.warning(f"End of partition reached: {msg.error()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                data = msg.value().decode("utf-8")
                logging.info(f"Consumed from Kafka topic '{TOPIC_NAME}': {data}")

    except KeyboardInterrupt:
        logging.info("Consumer interrupted by user")
    finally:
        consumer.close()
        logging.info("Consumer closed")


if __name__ == "__main__":
    logging.info("Starting Kafka Consumer...")
    consume_from_kafka()
