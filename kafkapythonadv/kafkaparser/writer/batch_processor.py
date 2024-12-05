import threading
import logging
from queue import Queue
from datetime import datetime, timedelta
import oss2
import time
import json
from threading import Timer, Lock
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Optional
from kafkaparser.transformer import apps, article, desktop, visitor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class OSSBatchProcessor:
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
        """Initialize the OSS batch processor with configuration and settings."""
        self.oss_bucket = oss2.Bucket(
            oss2.Auth(
                oss_config["oss_access_key_id"], oss_config["oss_access_key_secret"]
            ),
            oss_config["oss_endpoint"],
            oss_config["oss_bucket_name"],
        )
        self.base_path = base_path
        self.retry_attempts, self.retry_delay = retry_attempts, retry_delay
        self.logger = logging.getLogger("OSSBatchProcessor")

        # Buffer and batching
        self.buffer = bytearray()
        self.seq = 0
        self.max_buffer_size = max_buffer_size
        self.message_buffer = defaultdict(list)
        self.batch_interval = batch_interval

        # Event queue and threading
        self.event_queue = Queue(maxsize=max_queue_size)
        self.lock = threading.Lock()
        self.closed = False
        self.unloaded_uris = []

    def start(self):
        """Start background threads for processing events and periodic flushing."""
        self.logger.info("(0) Running the periodic flush")
        # threading.Thread(target=self._process_events, daemon=True).start()
        Timer(self.batch_interval, self._flush_batches_periodically).start()

    def stop(self):
        """Stop processing and flush any remaining buffered data."""
        with self.lock:
            self.closed = True
            self.event_queue.queue.clear()
            self._flush_buffer()  # Ensure no data is lost

    def add_message(self, topic, message):
        """Add a message to the buffer for a specific topic."""
        with self.lock:

            self.logger.info(f"(2) Appended {topic} message")

            self.message_buffer[topic].append(message)

    def add_source_data(self, uri):
        """Add a source data URI to the processing queue."""
        self._enqueue_event("add_source", {"uri": uri})

    def _enqueue_event(self, event_type, data):
        """Enqueue an event for asynchronous processing."""
        with self.lock:
            if not self.closed:
                self.event_queue.put({"type": event_type, "data": data})

    def _process_events(self):
        """Process events from the queue in a background thread."""
        while not (self.closed and self.event_queue.empty()):
            try:
                self.logger.info(self.event_queue)
                event = self.event_queue.get(timeout=1)
                if event["type"] == "add_source":
                    self.unloaded_uris.append(event["data"]["uri"])
                elif event["type"] == "load" and self.unloaded_uris:
                    self._load_to_oss(
                        self._generate_table_name(event["data"]["invoke_time"])
                    )
            except Exception as e:
                logging.error(f"Error processing events: {e}")

    def _generate_object_name(self):
        """Generate a unique object name for OSS storage."""
        timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H%M")
        self.seq += 1
        return f"{self.base_path}/{timestamp}/data-{self.seq:020d}.log"

    def _generate_table_name(self, invoke_time):
        """Generate a table name based on the invoke time."""
        time_slot = (invoke_time - timedelta(minutes=20)).replace(
            second=0, microsecond=0
        )
        return time_slot.strftime("%Y%m%d_%H%M")

    def _load_to_oss(self, table_name):
        """Load accumulated URIs into an OSS object."""
        try:
            content = "\n".join(self.unloaded_uris).encode("utf-8")
            self.unloaded_uris.clear()
            self._upload_with_retries(f"{table_name}.txt", content)
        except Exception as e:
            logging.error(f"Error uploading to OSS: {e}")

    def _upload_with_retries(self, object_name, data):
        """Upload data to OSS with retry logic."""
        for attempt in range(1, self.retry_attempts + 1):
            try:
                self.oss_bucket.put_object(object_name, data)
                logging.info(f"Uploaded {object_name}")
                return
            except Exception as e:
                logging.error(f"Upload attempt {attempt} failed: {e}")
                time.sleep(self.retry_delay)

    def add_data(self, data):
        """Add raw data to the buffer, flushing if necessary."""
        with self.lock:
            if len(self.buffer) + len(data) > self.max_buffer_size:
                self._flush_buffer()
            self.buffer.extend(data)

    def _flush_buffer(self):
        """Flush the current buffer to OSS and clear it."""
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

    def _flush_batches_periodically(self):
        """Periodically flush message batches to the buffer."""
        try:
            with self.lock:
                for topic, messages in list(self.message_buffer.items()):
                    
                    # self.logger.info(f"(4a) Checking message: {messages}")    ## Successfully checked
                    # self.logger.info(f"(4b) Checking topic: {topic}")         ## Successfully checked
                    
                    if messages:
                        try:
                            transformed_data = self._transform_messages(topic, messages)
                            
                            self.logger.info(f"(7) Data transformed successfully: {transformed_data}")
                            
                            if transformed_data:
                                self.add_data(transformed_data)
                        except Exception as e:
                            logging.error(f"Error flushing batch for {topic}: {e}")
                        finally:
                            self.message_buffer[topic].clear()
        except Exception as e:
            logging.error(f"Batch periodic flush error: {e}")
        finally:
            if not self.closed:
                Timer(self.batch_interval, self._flush_batches_periodically).start()

    def _transform_messages(
        self, topic: str, messages: List[Dict[str, Any]]
    ) -> Optional[bytes]:
        """Transform messages using the appropriate transformer. """
        transformed_data = []
        errors = []

        self.logger.info("(6a) messages are to be transformed...")
        
        for message in messages:
            message_str = json.dumps(message)
            self.logger.info(f"(6a) {type(message_str)} message to transform")
            try:
                if topic == "desktop":
                    desktop_doc = desktop.build_desktop_doc_from_byte_slice(message_str)
                    article_data, article_errs = (
                        article.extract_article_byte_slice_from_desktop_doc(desktop_doc)
                    )
                    visitor_data, visitor_errs = (
                        visitor.extract_visitor_byte_slice_from_desktop_doc(desktop_doc)
                    )
                    self.logger.info("Desktop data parsed successfully.")
                elif topic == "apps":
                    apps_doc = apps.build_apps_doc_from_byte_slice(message_str)
                    article_data, article_errs = (
                        article.extract_article_byte_slice_from_apps_doc(apps_doc)
                    )
                    visitor_data, visitor_errs = (
                        visitor.extract_visitor_byte_slice_from_apps_doc(apps_doc)
                    )
                    self.logger.info("Apps data parsed successfully.")
                else:
                    logging.warning(f"Unknown topic {topic}. Skipping transformation.")
                    continue

                # Log and collect errors if any
                if article_errs:
                    errors.extend(article_errs)
                if visitor_errs:
                    errors.extend(visitor_errs)

                # Combine article_data and visitor_data
                if article_data and visitor_data:
                    combined_data = [
                        {
                            **json.loads(a.decode("utf-8")),
                            **json.loads(v.decode("utf-8")),
                        }
                        for a, v in zip(article_data, visitor_data)
                    ]
                    transformed_data.extend(
                        [json.dumps(entry).encode("utf-8") for entry in combined_data]
                    )

            except Exception as e:
                errors.append(e)
                logging.error(f"Error processing message for topic {topic}: {e}")

        # if errors:
        #     logging.error(f"Errors occurred during topic {topic} message transformation: {errors}")

        if transformed_data:
            return b"\n".join(transformed_data)
        return None
