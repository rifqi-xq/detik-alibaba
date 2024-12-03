import time
import json
import logging
from threading import Thread, Event, Timer
from queue import Queue
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
import oss2
import setting

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class WorkerPool:
    def __init__(self, worker_count, max_queue_size=100):
        self.worker_count = worker_count
        self.job_queue = Queue(maxsize=max_queue_size)
        self.workers = []
        self.stop_event = Event()

    def start(self):
        for worker_id in range(1, self.worker_count + 1):
            worker = Thread(target=self._worker_task, args=(worker_id,))
            worker.daemon = True
            self.workers.append(worker)
            worker.start()
        logging.info(f"WorkerPool started with {self.worker_count} workers.")

    def stop(self):
        self.stop_event.set()
        for worker in self.workers:
            worker.join()
        logging.info("WorkerPool stopped.")

    def add_job(self, job):
        self.job_queue.put(job, block=False)

    def _worker_task(self, worker_id):
        logging.info(f"Worker {worker_id} started.")
        while not self.stop_event.is_set():
            try:
                job = self.job_queue.get(timeout=1)
                self.process_job(worker_id, job)
                self.job_queue.task_done()
            except:
                continue

    def process_job(self, worker_id, job):
        logging.info(f"Worker {worker_id} processing job: {job}")


class BatchProcessor:
    def __init__(self, oss_config, batch_interval=60):
        self.message_buffer = defaultdict(list)
        self.batch_interval = batch_interval
        self.oss_bucket = self.initialize_oss(oss_config)

    def initialize_oss(self, oss_config):
        logging.info("Initializing OSS connection...")
        auth = oss2.Auth(oss_config["oss_access_key_id"], oss_config["oss_access_key_secret"])
        return oss2.Bucket(auth, oss_config["oss_endpoint"], oss_config["oss_bucket_name"])

    def add_message_to_buffer(self, topic, message):
        self.message_buffer[topic].append(message)

    def store_batch(self):
        current_time = int(time.time())
        for topic, messages in self.message_buffer.items():
            if not messages:
                continue
            filename = f"{topic}_batch_{current_time}.json"
            batch_content = json.dumps(messages, indent=2).encode("utf-8")

            try:
                result = self.oss_bucket.put_object(filename, batch_content)
                if result.status == 200:
                    logging.info(f"Batch stored in OSS as {filename}")
                else:
                    logging.error(f"Failed to store batch for topic {topic}")
            except Exception as e:
                logging.error(f"Error uploading batch to OSS for topic {topic}: {e}")
            finally:
                self.message_buffer[topic].clear()

    def start_timer(self):
        Timer(self.batch_interval, self.store_batch).start()


class KafkaCollector:
    def __init__(self, kafka_config, topics, worker_pool, batch_processor):
        self.consumer = Consumer(kafka_config)
        self.topics = topics
        self.worker_pool = worker_pool
        self.batch_processor = batch_processor

    def start(self):
        logging.info("Starting Kafka collector...")
        self.consumer.subscribe(self.topics)
        self.batch_processor.start_timer()

        try:
            while True:
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
                    self.batch_processor.add_message_to_buffer(msg.topic(), message_data)
                    self.worker_pool.add_job({"topic": msg.topic(), "message": message_data})
                    logging.info(f"Message received from topic {msg.topic()}")
                except Exception as e:
                    logging.error(f"Error decoding message: {e}")
        except KeyboardInterrupt:
            logging.info("Stopping Kafka collector.")
        finally:
            self.consumer.close()

    def stop(self):
        self.consumer.close()
        logging.info("Kafka collector stopped.")


if __name__ == "__main__":
    kafka_conf = setting.kafka_setting
    oss_conf = setting.oss_setting
    
    kafka_config = {
        "bootstrap.servers": kafka_conf["bootstrap_servers"],
    }

    oss_config = {
        "oss_access_key_id": oss_conf["oss_access_key_id"],
        "oss_access_key_secret": oss_conf["oss_access_key_secret"],
        "oss_endpoint": oss_conf["oss_endpoint"],
        "oss_bucket_name": oss_conf["oss_bucket_name"],
    }

    topics = [kafka_conf["topic_name_01"], kafka_conf["topic_name_02"]]

    worker_pool = WorkerPool(worker_count=3)
    batch_processor = BatchProcessor(oss_config, batch_interval=60)
    kafka_collector = KafkaCollector(kafka_config, topics, worker_pool, batch_processor)

    try:
        worker_pool.start()
        kafka_collector.start()
    except KeyboardInterrupt:
        logging.info("Shutdown requested.")
        kafka_collector.stop()
        worker_pool.stop()
