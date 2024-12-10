import threading
import logging
from queue import Queue, Empty
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
        threading.Thread(target=self._process_events, daemon=True).start()
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
        self.logger.info(f"(12a) Added source data URI: {uri}")
        self._enqueue_event("add_source", {"uri": uri})

    def _enqueue_event(self, event_type, data):
        """Enqueue an event for asynchronous processing."""
        # with self.lock:
        self.logger.info("(12b) Adding an event...")
        if not self.closed:
            self.logger.info(f"(12b) Add to event queue: {event_type}")
            self.event_queue.put({"type": event_type, "data": data})

    def _process_events(self):
        """Process events from the queue in a background thread."""
        while not (self.closed and self.event_queue.empty()):
            try:
                event = self.event_queue.get(timeout=1)

                if not isinstance(event, dict) or "type" not in event or "data" not in event:
                    self.logger.error(f"Invalid event format: {event}")
                    continue

                with self.lock:
                    if event["type"] == "upload_json":
                        self.logger.info(f"Processing upload_json event: {event['data']}")
                        self._upload_json_to_oss(event["data"])
                    else:
                        self.logger.warning(f"Unsupported event type: {event['type']}")

                self.event_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error processing events: {e}")

    def _upload_json_to_oss(self, combined_data: List[Dict]):
        """Upload combined JSON data for multiple topics to OSS."""
        try:
            # Generate a unique object name with `.json` extension
            object_name = f"{self._generate_object_name()}.json"

            # Prepare the final JSON structure
            json_payload = {
                "timestamp": datetime.utcnow().isoformat(),
                "data": combined_data,
            }

            # Convert JSON payload to UTF-8 encoded bytes
            json_bytes = json.dumps(json_payload, ensure_ascii=False, indent=4).encode("utf-8")

            # Upload to OSS with retries
            self._upload_with_retries(object_name, json_bytes)
            self.logger.info(f"Successfully uploaded combined JSON data to OSS as {object_name}")
        except Exception as e:
            self.logger.error(f"Failed to upload combined JSON data to OSS: {e}")


    def _generate_object_name(self):
        """Generate a unique object name for OSS storage."""
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M")
        self.seq += 1
        return f"{timestamp}{self.seq:04d}"

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
                self.logger.info("(13) Adding data to OSS...")
                self.oss_bucket.put_object(object_name, data)
                logging.info(f"(13a) FINALLY!!! Uploaded {object_name}")
                return
            except Exception as e:
                logging.error(f"Upload attempt {attempt} failed: {e}")
                time.sleep(self.retry_delay)

    def add_data(self, data):
        """Add raw data to the buffer, flushing if necessary."""
        # self.logger.info(f"self.lock :: {self.lock}")
        # with self.lock:
        self.logger.info(f"len(self.buffer) + len(data) :: {len(self.buffer) + len(data)}")
        self.logger.info(f"self.max_buffer_size :: {self.max_buffer_size}")
        if len(self.buffer) + len(data) > self.max_buffer_size:
            self.logger.info("(9c) Flushing message buffers")
            self._flush_buffer()
        
        self.logger.info("(9a) Add to message buffer...")
        self.buffer.extend(data)
        self.logger.info("(9b) Message buffer added")

    def _flush_buffer(self):
        """Flush the current buffer to OSS and clear it."""
        # with self.lock:
        
        self.logger.info("(11) Initiating flush buffer...")
        self.logger.info(f"(11a) self.buffer :: {self.buffer}")
        
        if self.buffer:
            try:
                object_name = self._generate_object_name()
                
                self.logger.info(f"(11b) object_name :: {object_name}")
                self.add_source_data(object_name)
                self._enqueue_event("load", {"invoke_time": datetime.utcnow()})
                logging.info(f"(11c) Buffer flushed to {object_name}")
            except Exception as e:
                logging.error(f"Buffer flush failed: {e}")
            finally:
                self.buffer.clear()

    def _flush_batches_periodically(self):
        """Periodically flush message batches to the buffer."""
        try:
            combined_data = []

            for topic, messages in list(self.message_buffer.items()):
                if messages:
                    try:
                        transformed_data = self._transform_messages(topic, messages)
                        self.logger.info(f"(7a) Data transformed successfully for topic '{topic}'.")

                        if transformed_data:
                            # Load the transformed data as JSON
                            paired_data = json.loads(transformed_data.decode("utf-8"))

                            # Prepare topic-specific JSON payload
                            topic_json_data = [
                                {
                                    "topic": topic,
                                    "article": pair[0],
                                    "visitor": pair[1],
                                }
                                for pair in paired_data
                            ]
                            combined_data.extend(topic_json_data)

                    except Exception as e:
                        self.logger.error(f"Error flushing batch for topic '{topic}': {e}")
                    finally:
                        self.logger.info(f"(8a) Clearing messages for topic '{topic}'.")
                        self.message_buffer[topic].clear()
                else:
                    self.logger.info(f"(7b) No messages to process for topic '{topic}'.")

            if combined_data:
                # Enqueue a single event to upload the combined JSON data
                self._enqueue_event("upload_json", combined_data)

        except Exception as e:
            self.logger.error(f"Batch periodic flush error: {e}")
        finally:
            if not self.closed:
                self.logger.info("(8b) Recursive batch flush starting.")
                Timer(self.batch_interval, self._flush_batches_periodically).start()


    def _transform_messages(
        self, topic: str, messages: List[Dict[str, Any]]
    ) -> Optional[bytes]:
        """Transform messages using the appropriate transformer. """
        transformed_data = []
        errors = []
        
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

                #    Log and collect errors if any
                if article_errs:
                    errors.extend(article_errs)
                if visitor_errs:
                    errors.extend(visitor_errs)

                # Pair each article data with its corresponding visitor data
                if article_data and visitor_data and len(article_data) == len(visitor_data):
                    paired_data = [
                        [json.loads(a.decode("utf-8")), json.loads(v.decode("utf-8"))]
                        for a, v in zip(article_data, visitor_data)
                    ]
                    transformed_data.extend(paired_data)
                else:
                    logging.warning(
                        f"Mismatch in lengths or missing data for article and visitor on topic {topic}."
                    )

            except Exception as e:
                errors.append(e)
                logging.error(f"Error processing message for topic {topic}: {e}")

        if errors:
            logging.error(f"Errors occurred during message transformation: {errors}")

        if transformed_data:
            # Convert the list of lists into a JSON-formatted bytes object
            return json.dumps(transformed_data).encode("utf-8")
        return None
