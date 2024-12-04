import asyncio
import logging
from threading import Thread
from consumer.kafka_consumer import KafkaConsumerService
from producer.kafka_producer import KafkaProducerService
from kafkaparser.transformer import apps, article, desktop, visitor

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
            "apps_doc": apps.build_apps_doc_from_byte_slice,
            "desktop_doc": desktop.build_desktop_doc_from_byte_slice,
            "article_byte_apps": article.extract_article_byte_slice_from_apps_doc,
            "article_byte_desktop": article.extract_article_byte_slice_from_desktop_doc,
            "visitor_byte_apps": article.extract_article_byte_slice_from_apps_doc,
            "visitor_byte_desktop": article.extract_article_byte_slice_from_desktop_doc,            
        }

    def start(self):
        """Start the service and its components."""
        self.logger.info("Starting service...")
        self.consumer_thread = Thread(target=self._run_consumer)
        self.consumer_thread.start()
        self.logger.info("Service started.")

    def _run_consumer(self):
        asyncio.run(self.kafka_consumer.start())

    def produce_data(self, raw_data: bytes):
        try:
            self.kafka_producer.produce_data(raw_data)
            self.logger.info("Data produced to Kafka.")
        except ValueError as e:
            self.logger.error(f"Failed to transform data: {e}")

    def stop(self):
        """Stop the service and its components."""
        self.logger.info("Stopping service...")
        self.kafka_consumer.stop()
        if self.consumer_thread:
            self.consumer_thread.join()
        self.kafka_producer.flush()
        self.logger.info("Service stopped.")
