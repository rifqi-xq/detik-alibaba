import json
import logging
from threading import Event
from confluent_kafka import Consumer, KafkaError
import setting
from ..parser.writer.worker_pool import WorkerPool  # Import WorkerPool from existing code
from ..parser.writer.batch_processor import BatchProcessor  # Import BatchProcessor
from ..parser.writer.oss_writer import OSSWriter  # Import the integrated OSSWriter

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class KafkaCollector:
    def __init__(self, kafka_config, topics, worker_pool, batch_processor):
        self.consumer = Consumer(kafka_config)
        self.topics = topics
        self.worker_pool = worker_pool
        self.batch_processor = batch_processor
        self.stop_event = Event()

    def start(self):
        """Start the Kafka collector."""
        logging.info("Starting Kafka collector...")
        self.consumer.subscribe(self.topics)

        try:
            while not self.stop_event.is_set():
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error(f"Consumer error: {msg.error()}")
                        continue

                try:
                    message_str = msg.value().decode("utf-8")
                    message_data = json.loads(message_str)

                    # Add message to BatchProcessor buffer
                    self.batch_processor.add_message_to_buffer(msg.topic(), message_data)

                    # Submit processing job to WorkerPool
                    self.worker_pool.add_job({"topic": msg.topic(), "message": message_data})
                    logging.info(f"Message received and job added for topic {msg.topic()}")
                except Exception as e:
                    logging.error(f"Error decoding message: {e}")
        except KeyboardInterrupt:
            logging.info("Stopping Kafka collector.")
        finally:
            self.stop()

    def stop(self):
        """Stop the Kafka collector gracefully."""
        self.stop_event.set()
        self.consumer.close()
        self.batch_processor.store_batch()  # Final flush of buffered messages
        logging.info("Kafka collector stopped.")

if __name__ == "__main__":
    # Configuration settings
    kafka_conf = setting.kafka_setting
    oss_conf = setting.oss_setting

    kafka_config = {
        "bootstrap.servers": kafka_conf["bootstrap_servers"],
        "group.id": "consumer_group_1",
    }

    oss_config = {
        "oss_access_key_id": oss_conf["oss_access_key_id"],
        "oss_access_key_secret": oss_conf["oss_access_key_secret"],
        "oss_endpoint": oss_conf["oss_endpoint"],
        "oss_bucket_name": oss_conf["oss_bucket_name"],
    }

    topics = [kafka_conf["topic_name_01"], kafka_conf["topic_name_02"]]

    # Initialize WorkerPool
    worker_pool = WorkerPool(worker_count=3)

    # Initialize OSSWriter
    writer = OSSWriter(name="kafka_to_oss_writer", oss_config=oss_config)

    # Initialize BatchProcessor with OSSWriter
    batch_processor = BatchProcessor(writer=writer, batch_interval=60)

    # Start Kafka collector
    kafka_collector = KafkaCollector(kafka_config, topics, worker_pool, batch_processor)
    try:
        worker_pool.start()
        batch_processor.start_timer()
        kafka_collector.start()
    except KeyboardInterrupt:
        logging.info("Shutdown requested.")
        kafka_collector.stop()
        worker_pool.stop()