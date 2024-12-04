import json
import logging
import asyncio
from threading import Event
from setting import kafka_setting, oss_setting
from confluent_kafka import Consumer, KafkaError
from kafkaparser.writer.scheduler import Scheduler, round_up_15_minutes
from kafkaparser.writer.buffer_writer import BufferWriter
from kafkaparser.writer.worker_pool import WorkerPool
from kafkaparser.writer.batch_processor import BatchProcessor
from kafkaparser.writer.oss_writer import OSSWriter

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

        oss_writer = OSSWriter(name="kafka_to_oss_writer", oss_config=oss_config)
        buffer_writer = BufferWriter(oss_writer, base_path=base_path)
        self.batch_processor = BatchProcessor(
            buffer_writer=buffer_writer, batch_interval=batch_interval
        )
        self.worker_pool = WorkerPool(worker_count=worker_count)

        self.scheduler = Scheduler(next_time_func=round_up_15_minutes)
        self.scheduler_queue = self.scheduler.register()
        self.logger = logging.getLogger("KafkaConsumerService")

    async def start(self):
        self.consumer.subscribe(self.topics)
        self.worker_pool.start()
        self.scheduler.start()
        try:
            await asyncio.gather(self._consume_messages(), self._periodic_flush())
        finally:
            self.stop()

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
                self.batch_processor.add_message_to_buffer(msg.topic(), message_data)
                self.worker_pool.add_job(
                    {"topic": msg.topic(), "message": message_data}
                )
                self.logger.info(
                    f"Message received and job added for topic {msg.topic()}"
                )
            except Exception as e:
                self.logger.error(f"Error decoding message: {e}")

    async def _periodic_flush(self):
        while True:
            next_flush = await self.scheduler_queue.get()
            if next_flush is None:
                break
            self.batch_processor.store_batch()

    def stop(self):
        self.stop_event.set()
        self.consumer.close()
        self.batch_processor.store_batch()
        self.scheduler.stop()
        self.worker_pool.stop()
        self.logger.info("Kafka consumer stopped.")


if __name__ == "__main__":

    async def main():
        kafka_config = {
            "bootstrap.servers": kafka_setting["bootstrap_servers"],
            "group.id": kafka_setting["group_name"],
        }
        oss_config = {
            "oss_access_key_id": oss_setting["oss_access_key_id"],
            "oss_access_key_secret": oss_setting["oss_access_key_secret"],
            "oss_endpoint": oss_setting["oss_endpoint"],
            "oss_bucket_name": oss_setting["oss_bucket_name"],
        }
        topics = [kafka_setting["topic_name_01"], kafka_setting["topic_name_02"]]
        consumer_service = KafkaConsumerService(
            kafka_config=kafka_config,
            topics=topics,
            oss_config=oss_config,
            base_path="data/logs",
            worker_count=3,
            batch_interval=60,
        )
        await consumer_service.start()

    asyncio.run(main())
