import json
import logging
import time
from threading import Event
from confluent_kafka import Consumer, KafkaError
from datetime import datetime, timedelta
from queue import Queue
import oss2
import setting

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# OSS Writer with buffering capabilities
class OSSWriter:
    def __init__(self, name, oss_config):
        self.name = name
        self.oss_bucket = self.initialize_oss(oss_config)

    @staticmethod
    def initialize_oss(oss_config):
        auth = oss2.Auth(oss_config["oss_access_key_id"], oss_config["oss_access_key_secret"])
        bucket = oss2.Bucket(auth, oss_config["oss_endpoint"], oss_config["oss_bucket_name"])
        return bucket

    def upload_data(self, object_name, data):
        """Upload data to OSS."""
        self.oss_bucket.put_object(object_name, data)
        logging.info(f"Data uploaded to OSS as {object_name}")

class BufferWriter:
    def __init__(self, oss_writer, base_path, max_size=10 * 1024 * 1024):
        self.oss_writer = oss_writer
        self.base_path = base_path
        self.max_size = max_size
        self.buffer = bytearray()
        self.seq = 0

    def new_object_name(self, timestamp):
        """Generate a new OSS object name based on time and sequence."""
        window_path = timestamp.strftime("%Y/%m/%d/%H%M")
        object_name = f"{self.base_path}/{window_path}/data-{self.seq:020d}.log"
        self.seq += 1
        return object_name

    def flush(self):
        """Flush buffer to OSS."""
        if self.buffer:
            object_name = self.new_object_name(datetime.utcnow())
            self.oss_writer.upload_data(object_name, self.buffer)
            self.buffer.clear()

    def add_data(self, data):
        """Add data to the buffer and flush if needed."""
        if len(self.buffer) + len(data) > self.max_size:
            self.flush()
        self.buffer.extend(data)

class BatchProcessor:
    def __init__(self, writer, batch_interval=60):
        self.writer = writer
        self.buffer = {}
        self.batch_interval = batch_interval
        self.last_flush_time = time.time()

    def add_message_to_buffer(self, topic, message):
        """Add message to topic-specific buffer."""
        if topic not in self.buffer:
            self.buffer[topic] = []
        self.buffer[topic].append(message)

    def store_batch(self):
        """Store all messages from the buffer."""
        for topic, messages in self.buffer.items():
            if messages:
                data = "\n".join(json.dumps(msg) for msg in messages).encode("utf-8")
                self.writer.add_data(data)
                logging.info(f"Stored {len(messages)} messages for topic {topic}")
        self.buffer.clear()

    def start_timer(self):
        """Start a timer to periodically flush the batch."""
        def flush_periodically():
            while True:
                time.sleep(self.batch_interval)
                self.store_batch()

        from threading import Thread
        Thread(target=flush_periodically, daemon=True).start()

class WorkerPool:
    def __init__(self, worker_count=3):
        self.worker_count = worker_count
        self.jobs = Queue()
        self.stop_event = Event()

    def add_job(self, job):
        """Add a job to the queue."""
        self.jobs.put(job)

    def _worker(self):
        """Worker thread to process jobs."""
        while not self.stop_event.is_set():
            try:
                job = self.jobs.get(timeout=1)
                logging.info(f"Processing job for topic {job['topic']}")
                # Process job (e.g., further transformation if needed)
                self.jobs.task_done()
            except Exception as e:
                continue

    def start(self):
        """Start worker threads."""
        for _ in range(self.worker_count):
            from threading import Thread
            Thread(target=self._worker, daemon=True).start()

    def stop(self):
        """Stop worker threads."""
        self.stop_event.set()

# Kafka Collector with integrated processing pipeline
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

    # Initialize OSSWriter
    oss_writer = OSSWriter(name="kafka_to_oss_writer", oss_config=oss_config)

    # Initialize BufferWriter with OSSWriter
    buffer_writer = BufferWriter(oss_writer, base_path="data/logs")

    # Initialize BatchProcessor with BufferWriter
    batch_processor = BatchProcessor(writer=buffer_writer, batch_interval=60)

    # Initialize WorkerPool
    worker_pool = WorkerPool(worker_count=3)

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
