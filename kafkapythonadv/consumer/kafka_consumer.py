import json
import logging
import asyncio
from threading import Event
from setting import kafka_setting, oss_setting
from confluent_kafka import Consumer, KafkaError
from kafkaparser.writer.batch_processor import OSSBatchProcessor
from kafkaparser.writer.worker_pool import WorkerPool

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class KafkaConsumerService:
    def __init__(
        self, kafka_config, topics, oss_config, base_path, worker_count, batch_interval
    ):
        self.consumer = Consumer(
            {
                "bootstrap.servers": kafka_config["bootstrap_servers"],
                "group.id": kafka_config["group_name"],
                "auto.offset.reset": "latest",
            }
        )
        self.topics = topics
        self.stop_event = Event()
        self.batch_processor = OSSBatchProcessor(
            oss_config=oss_config, base_path=base_path, batch_interval=batch_interval
        )
        self.worker_pool = WorkerPool(worker_count=worker_count)
        self.logger = logging.getLogger("KafkaConsumerService")

    async def _consume_messages(self):
        while not self.stop_event.is_set():
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    self.logger.error(f"Consumer error: {msg.error()}")
                continue
            try:
                message_data = json.loads(msg.value().decode("utf-8"))
                self.batch_processor.add_message(msg.topic(), message_data)
                self.worker_pool.add_job(
                    {"topic": msg.topic(), "message": message_data}
                )
                self.logger.info(
                    f"Message received and job added for topic {msg.topic()}"
                )
            except Exception as e:
                self.logger.error(f"Error decoding message: {e}")

    async def start(self):
        self.consumer.subscribe(self.topics)
        self.worker_pool.start()
        try:
            await self._consume_messages()
        finally:
            self.stop()

    def stop(self):
        self.stop_event.set()
        self.consumer.close()
        self.batch_processor.store_batch()
        self.worker_pool.stop()
        self.logger.info("Kafka consumer stopped.")
