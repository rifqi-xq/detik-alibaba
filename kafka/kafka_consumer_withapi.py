from confluent_kafka import Consumer, KafkaError
import oss2
import setting
import time
from threading import Timer
from collections import defaultdict

# Configuration
kafka_conf = setting.kafka_setting
oss_conf = setting.oss_setting
message_buffer = defaultdict(list)  # Stores messages by topic
batch_interval = 1 * 60  # 1 minutes


def create_kafka_consumer():
    return Consumer(
        {
    'bootstrap.servers': kafka_conf['bootstrap_servers'],
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms':'PLAIN',
    'ssl.ca.location':kafka_conf['ca_location'],
    'security.protocol':'SASL_SSL',
    'sasl.username':kafka_conf['sasl_plain_username'],
    'sasl.password':kafka_conf['sasl_plain_password'],
    'ssl.endpoint.identification.algorithm':'none',
    'group.id': kafka_conf['group_name'],
    'auto.offset.reset': 'latest',
    'fetch.message.max.bytes':'1024*512'
}
    )


def initialize_oss_connection():
    auth = oss2.Auth(oss_conf["oss_access_key_id"], oss_conf["oss_access_key_secret"])
    return oss2.Bucket(auth, oss_conf["oss_endpoint"], oss_conf["oss_bucket_name"])


def add_message_to_buffer(topic: str, message: str):
    message_buffer[topic].append(message)


def store_batch_in_oss(bucket):
    current_time = int(time.time())

    for topic, messages in message_buffer.items():
        if messages:
            # Create a filename based on topic and timestamp
            filename = f"{topic}_batch_{current_time}.txt"
            batch_content = "\n".join(messages).encode("utf-8")

            # Attempt to upload the batch to OSS
            try:
                result = bucket.put_object(filename, batch_content)
                if result.status == 200:
                    print(f"Batch stored in OSS as {filename}")
                else:
                    print(f"Failed to store batch for topic {topic} in OSS")
            except Exception as e:
                print(f"Error uploading to OSS: {e}")

            # Clear the buffer for this topic after uploading
            message_buffer[topic].clear()

    # Restart the timer for the next batch
    start_batch_timer(bucket)


def start_batch_timer(bucket):
    """Start a timer to trigger batch storage every batch_interval."""
    Timer(batch_interval, store_batch_in_oss, [bucket]).start()


def main():
    consumer = create_kafka_consumer()
    bucket = initialize_oss_connection()

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
                    print(f"Consumer error: {msg.error()}")
                    continue

            # Decode and buffer the message by topic
            try:
                message_str = msg.value().decode("utf-8")
                add_message_to_buffer(msg.topic(), message_str)
            except Exception as e:
                print(f"Error decoding message: {e}")

    except KeyboardInterrupt:
        print("Stopping consumer.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
