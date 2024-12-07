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
batch_interval = 1 * 60 
# MAX_MESSAGES_PER_BATCH = 100 

def create_kafka_consumer():
    logging.info("Initializing Kafka consumer...")
    return Consumer(
        {
            "bootstrap.servers": kafka_conf["bootstrap_servers"],
            # "ssl.endpoint.identification.algorithm": "none",
            # "sasl.mechanisms": "PLAIN",
            # "ssl.ca.location": kafka_conf["ca_location"],
            # "security.protocol": "SASL_SSL",
            # "sasl.username": kafka_conf["sasl_plain_username"],
            # "sasl.password": kafka_conf["sasl_plain_password"],
            "group.id": kafka_conf["group_name"],
            "auto.offset.reset": "latest",
        }
    )

def initialize_oss_connection():
    logging.info("Initializing OSS connection...")
    auth = oss2.Auth(oss_conf["oss_access_key_id"], oss_conf["oss_access_key_secret"])
    return oss2.Bucket(auth, oss_conf["oss_endpoint"], oss_conf["oss_bucket_name"])

def add_message_to_buffer(topic: str, message):
    # print(f"messagenya yg ini: {message}")
    message_buffer[topic].append(message)

def store_batch_in_oss(bucket, topic=None):
    current_time = int(time.time())
    topics_to_process = [topic]

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
    Timer(batch_interval, store_batch_in_oss, [bucket]).start()

def main():
    global bucket
    consumer = create_kafka_consumer()
    bucket = initialize_oss_connection()

    # Subscribe to the Kafka topic
    topics = [kafka_conf["topic_name_01"], kafka_conf["topic_name_02"]]
    consumer.subscribe(topics)
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
                # Decode the Kafka message and parse the JSON
                message_str = msg.value().decode("utf-8")
                message_data = json.loads(message_str)
                add_message_to_buffer(msg.topic(), message_data)
                logging.info(f"Received message from topic {msg.topic()}")
            except Exception as e:
                logging.error(f"Error decoding message: {e}")

    except KeyboardInterrupt:
        logging.info("Stopping Kafka consumer.")

    finally:
        consumer.close()
        logging.info("Kafka consumer closed.")

if __name__ == "__main__":
    main()
