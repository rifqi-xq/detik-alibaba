import asyncio
import logging
from threading import Thread
from kafkapythonadv.consumer.kafka_consumer import KafkaConsumerService
from kafkapythonadv.producer.kafka_producer import KafkaProducerService

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

    def start(self):
        """Start the service and its components."""
        self.logger.info("Starting service...")
        self.consumer_thread = Thread(target=self._run_consumer)
        self.consumer_thread.start()
        self.logger.info("Service started.")

    def _run_consumer(self):
        asyncio.run(self.kafka_consumer.start())

    def produce_data(self, data):
        """Produce data to Kafka using the producer service."""
        self.kafka_producer.produce_data(data)

    def stop(self):
        """Stop the service and its components."""
        self.logger.info("Stopping service...")
        self.kafka_consumer.stop()
        if self.consumer_thread:
            self.consumer_thread.join()
        self.kafka_producer.flush()
        self.logger.info("Service stopped.")
