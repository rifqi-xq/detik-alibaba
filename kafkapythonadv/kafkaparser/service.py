import asyncio
import logging
from threading import Thread
from consumer.kafka_consumer import KafkaConsumerService
from producer.kafka_producer import KafkaProducerService
from transformer import build_desktop_doc_from_byte_slice, build_apps_doc_from_byte_slice  # Import multiple transformers

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Service:
    def __init__(self, kafka_config, consumer_topics, oss_config, base_path, worker_count, batch_interval):
        self.logger = logging.getLogger("Service")
        self.kafka_producer = KafkaProducerService(kafka_config)
        self.kafka_consumer = KafkaConsumerService(
            kafka_config=kafka_config,
            topics=consumer_topics,
            oss_config=oss_config,
            base_path=base_path,
            worker_count=worker_count,
            batch_interval=batch_interval,
        )
        self.consumer_thread = None

        # Dictionary to hold transformers and their associated producer IDs
        self.transformers = {
            "desktop": build_desktop_doc_from_byte_slice,
            "apps": build_apps_doc_from_byte_slice,
        }

    def start(self):
        """Start the service and its components."""
        self.logger.info("Starting service...")
        self.consumer_thread = Thread(target=self._run_consumer)
        self.consumer_thread.start()
        self.logger.info("Service started.")

    def _run_consumer(self):
        asyncio.run(self.kafka_consumer.start())

    def transform_and_produce_data(self, raw_data: bytes, transformer_key: str):
        """
        Apply a specific transformation to raw data and produce transformed data to Kafka.

        :param raw_data: JSON-encoded byte string containing data.
        :param transformer_key: Key to select the appropriate transformer.
        """
        transformer = self.transformers.get(transformer_key)
        if not transformer:
            self.logger.error(f"Transformer '{transformer_key}' not found.")
            return

        try:
            # Apply the selected transformation
            transformed_data = transformer(raw_data)
            self.logger.info(f"Transformed Data with {transformer_key}: {transformed_data}")

            # Produce transformed data
            self.kafka_producer.produce_data(transformed_data)
            self.logger.info(f"Transformed data ({transformer_key}) produced to Kafka.")
        except ValueError as e:
            self.logger.error(f"Failed to transform data using '{transformer_key}': {e}")

    def stop(self):
        """Stop the service and its components."""
        self.logger.info("Stopping service...")
        self.kafka_consumer.stop()
        if self.consumer_thread:
            self.consumer_thread.join()
        self.kafka_producer.flush()
        self.logger.info("Service stopped.")
