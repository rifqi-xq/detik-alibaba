import threading
import logging
from queue import Queue
from datetime import datetime, timedelta
import oss2
import time
import json
from threading import Timer, Lock
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class OSSBatchProcessor:
    # Initialize the OSSBatchProcessor with configuration and buffering options
    def __init__(
        self,
        oss_config,
        base_path,
        max_buffer_size=10 * 1024 * 1024,
        max_queue_size=64,
        retry_attempts=3,
        retry_delay=5,
        batch_interval=60,
    ):
        self.oss_bucket = oss2.Bucket(
            oss2.Auth(
                oss_config["oss_access_key_id"], oss_config["oss_access_key_secret"]
            ),
            oss_config["oss_endpoint"],
            oss_config["oss_bucket_name"],
        )
        self.base_path = base_path
        self.retry_attempts, self.retry_delay = retry_attempts, retry_delay

        self.buffer = bytearray()
        self.seq = 0
        self.max_buffer_size = max_buffer_size
        self.message_buffer = defaultdict(list)
        self.batch_interval = batch_interval

        self.event_queue = Queue(maxsize=max_queue_size)
        self.lock = threading.Lock()
        self.closed = False
        self.unloaded_uris = []

    # Add a message to the buffer for a specific topic
    def add_message(self, topic, message):
        with self.lock:
            self.message_buffer[topic].append(message)

    # Add a source data URI to the event queue
    def add_source_data(self, uri):
        self._enqueue_event("add_source", {"uri": uri})

    # Enqueue a new event with its type and data
    def _enqueue_event(self, event_type, data):
        with self.lock:
            if not self.closed:
                self.event_queue.put({"type": event_type, "data": data})

    # Process events from the event queue
    def _process_events(self):
        while not (self.closed and self.event_queue.empty()):
            try:
                event = self.event_queue.get(timeout=1)
                if event["type"] == "add_source":
                    self.unloaded_uris.append(event["data"]["uri"])
                elif event["type"] == "load" and self.unloaded_uris:
                    self._load_to_oss(
                        self._generate_table_name(event["data"]["invoke_time"])
                    )
            except Exception as e:
                logging.error(f"Error processing events: {e}")

    # Generate a unique object name based on timestamp and sequence number
    def _generate_object_name(self):
        timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H%M")
        self.seq += 1
        return f"{self.base_path}/{timestamp}/data-{self.seq:020d}.log"

    # Generate a table name based on invocation time
    def _generate_table_name(self, invoke_time):
        time_slot = (invoke_time - timedelta(minutes=20)).replace(
            second=0, microsecond=0
        )
        return time_slot.strftime("%Y%m%d_%H%M")

    # Load data into OSS storage with retries
    def _load_to_oss(self, table_name):
        try:
            content = "\n".join(self.unloaded_uris).encode("utf-8")
            self.unloaded_uris.clear()
            self._upload_with_retries(f"{table_name}.txt", content)
        except Exception as e:
            logging.error(f"Error uploading to OSS: {e}")

    # Upload data to OSS with retry mechanism
    def _upload_with_retries(self, object_name, data):
        for attempt in range(1, self.retry_attempts + 1):
            try:
                self.oss_bucket.put_object(object_name, data)
                logging.info(f"Uploaded {object_name}")
                return
            except Exception as e:
                logging.error(f"Upload attempt {attempt} failed: {e}")
                time.sleep(self.retry_delay)

    # Add raw data to the buffer, flushing if necessary
    def add_data(self, data):
        with self.lock:
            if len(self.buffer) + len(data) > self.max_buffer_size:
                self._flush_buffer()
            self.buffer.extend(data)

    # Flush the buffer and upload its content
    def _flush_buffer(self):
        with self.lock:
            if self.buffer:
                try:
                    object_name = self._generate_object_name()
                    self.add_source_data(object_name)
                    self._enqueue_event("load", {"invoke_time": datetime.utcnow()})
                    logging.info(f"Buffer flushed to {object_name}")
                except Exception as e:
                    logging.error(f"Buffer flush failed: {e}")
                finally:
                    self.buffer.clear()

    # Periodically flush batches of messages to the buffer
    def _flush_batches_periodically(self):
        try:
            with self.lock:
                for topic, messages in list(self.message_buffer.items()):
                    if messages:
                        try:
                            content = json.dumps(messages, indent=2).encode("utf-8")
                            self.add_data(content)
                        except Exception as e:
                            logging.error(f"Error flushing batch for {topic}: {e}")
                        finally:
                            self.message_buffer[topic].clear()
        except Exception as e:
            logging.error(f"Batch periodic flush error: {e}")
        finally:
            if not self.closed:
                Timer(self.batch_interval, self._flush_batches_periodically).start()

    # Start processing events and batching periodically
    def start(self):
        threading.Thread(target=self._process_events, daemon=True).start()
        Timer(self.batch_interval, self._flush_batches_periodically).start()

    # Stop processing and flush remaining buffers
    def stop(self):
        with self.lock:
            self.closed = True
            self.event_queue.queue.clear()
            self._flush_buffer()  # Ensure no data is lost
