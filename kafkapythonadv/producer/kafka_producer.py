from confluent_kafka import Producer, KafkaException
import json
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

class KafkaProducerService:
    def __init__(self, kafka_config):
        self.producer = Producer({"bootstrap.servers": kafka_config["bootstrap_servers"]})
        self.topic_apps = kafka_config["topic_name_01"]
        self.topic_desktop = kafka_config["topic_name_02"]
        self.logger = logging.getLogger("KafkaProducerService")

    def detect_topic(self, data):
        if "device_brand" in data:
            return self.topic_apps
        elif "thumbnailUrl" in data:
            return self.topic_desktop
        else:
            return "unknown"

    def produce_data(self, data):
        try:
            timestamp = int(time.time())
            data["timestamp"] = timestamp

            topic = self.detect_topic(data)
            if topic == "unknown":
                self.logger.warning("Topic detection failed; skipping message.")
                return

            message = json.dumps(data).encode("utf-8")
            self.producer.produce(
                topic,
                message,
                callback=self.delivery_report
            )
            self.producer.poll(0)
            self.logger.info(f"Data sent to topic {topic}.")
        except KafkaException as e:
            self.logger.error(f"Kafka error: {e}")
        except Exception as e:
            self.logger.error(f"Error producing data: {e}")

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def flush(self):
        self.producer.flush()
        self.logger.info("Kafka producer flushed.")
