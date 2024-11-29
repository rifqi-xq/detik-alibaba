import time
import json
from confluent_kafka import Consumer, KafkaError
import oss2
import setting
from threading import Timer
from collections import defaultdict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load configurations
kafka_conf = setting.kafka_setting
oss_conf = setting.oss_setting

# Buffer for messages grouped by topic
message_buffer = defaultdict(list)
BATCH_INTERVAL = 60  # 1 minute in seconds
MAX_MESSAGES_PER_BATCH = 100  # Maximum messages per batch to avoid oversized files

def create_kafka_consumer():
    logging.info("Initializing Kafka consumer...")
    return Consumer(
        {
            "bootstrap.servers": kafka_conf["bootstrap_servers"],
            "ssl.endpoint.identification.algorithm": "none",
            "sasl.mechanisms": "PLAIN",
            "ssl.ca.location": kafka_conf["ca_location"],
            "security.protocol": "SASL_SSL",
            "sasl.username": kafka_conf["sasl_plain_username"],
            "sasl.password": kafka_conf["sasl_plain_password"],
            "group.id": kafka_conf["group_name"],
            "auto.offset.reset": "latest",
            "fetch.message.max.bytes": "524288",  # 512 KB
        }
    )

def initialize_oss_connection():
    logging.info("Initializing OSS connection...")
    auth = oss2.Auth(oss_conf["oss_access_key_id"], oss_conf["oss_access_key_secret"])
    return oss2.Bucket(auth, oss_conf["oss_endpoint"], oss_conf["oss_bucket_name"])

def add_message_to_buffer(topic: str, message):
    # message_buffer[topic].append(message)
    print(f"messagenya yg ini: {message}")
    message_buffer.append(message)
    # if len(message_buffer[topic]) >= MAX_MESSAGES_PER_BATCH:
    if len(message_buffer) >= MAX_MESSAGES_PER_BATCH:
        logging.info(f"Max messages reached for topic {topic}. Triggering batch upload.")
        store_batch_in_oss(bucket, topic)

def store_batch_in_oss(bucket, topic=None):
    current_time = int(time.time())

    if topic:
        # Handle a specific topic's batch
        topics_to_process = [topic]
    else:
        # Handle all topics if no topic specified
        topics_to_process = list(message_buffer.keys())

    for topic in topics_to_process:
        messages = message_buffer
        print(f"Check message: {messages}")
        if messages:
            filename = f"{topic}_batch_{current_time}.json"
            batch_content = json.dumps(messages, indent=2).encode("utf-8")
            
            try:
                print(batch_content)
                result = bucket.put_object(filename, batch_content)
                if result.status == 200:
                    logging.info(f"Successfully stored batch in OSS as {filename}")
                else:
                    logging.error(f"Failed to store batch for topic {topic} in OSS")
            except Exception as e:
                logging.error(f"Error uploading batch to OSS for topic {topic}: {e}")
            finally:
                # Clear buffer after upload
                message_buffer[topic].clear()

def start_batch_timer(bucket):
    Timer(BATCH_INTERVAL, store_batch_in_oss, [bucket]).start()

def main():
    global bucket
    consumer = create_kafka_consumer()
    bucket = initialize_oss_connection()

    # Subscribe to the Kafka topic
    consumer.subscribe([kafka_conf["topic_name"]])
    start_batch_timer(bucket)

    try:
        while True:
            # Poll messages from Kafka
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                    continue

            # Decode and buffer the message
            try:
                # message_str = msg.value().decode("utf-8")
                # Decode the Kafka message and parse the JSON
                message_bytes = msg.value()  # Raw bytes from Kafka
                message_str = message_bytes.decode('utf-8')  # Convert bytes to string
                try:
                    # Parse the JSON string into a Python dictionary
                    message_data = json.loads(message_str)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    message_data = {"raw_data": message_str}  # Fallback in case of parsing error

                add_message_to_buffer(msg.topic(), message_data)
                logging.info(f"Received message from topic {msg.topic()}: {message_data}")
            except Exception as e:
                logging.error(f"Error decoding message: {e}")

    except KeyboardInterrupt:
        logging.info("Stopping Kafka consumer.")

    finally:
        consumer.close()
        logging.info("Kafka consumer closed.")

if __name__ == "__main__":
    main()
