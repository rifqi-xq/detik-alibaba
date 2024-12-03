import threading
import logging
from queue import Queue
from datetime import datetime, timedelta
import oss2

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class OSSWriter:
    EVENT_ADD_SOURCE = "add_source"
    EVENT_LOAD = "load"

    def __init__(self, name, oss_config, max_queue_size=64):
        self.name = name
        self.closed = False

        self.oss_bucket = self.initialize_oss(oss_config)
        self.unloaded_uris = []

        self.event_queue = Queue(maxsize=max_queue_size)
        self.lock = threading.Lock()
        self.event_empty_cond = threading.Condition(self.lock)
        self.event_full_cond = threading.Condition(self.lock)

    @staticmethod
    def initialize_oss(oss_config):
        auth = oss2.Auth(oss_config["oss_access_key_id"], oss_config["oss_access_key_secret"])
        bucket = oss2.Bucket(auth, oss_config["oss_endpoint"], oss_config["oss_bucket_name"])
        return bucket

    def start(self):
        """Starts the writer in a background thread."""
        threading.Thread(target=self._process_events, daemon=True).start()

    def stop(self):
        """Stops the writer gracefully."""
        with self.lock:
            self.closed = True
            self.event_empty_cond.notify_all()
            self.event_full_cond.notify_all()

    def add_source_data(self, uri):
        """Adds a source URI to the event queue."""
        with self.lock:
            while self.event_queue.full() and not self.closed:
                self.event_full_cond.wait()

            if self.closed:
                raise RuntimeError("Writer has been closed")

            self.event_queue.put({"type": self.EVENT_ADD_SOURCE, "data": {"uri": uri}})
            self.event_empty_cond.notify_all()

    def load(self, invoke_time=None):
        """Triggers a load event."""
        with self.lock:
            while self.event_queue.full() and not self.closed:
                self.event_full_cond.wait()

            if self.closed:
                raise RuntimeError("Writer has been closed")

            invoke_time = invoke_time or datetime.utcnow()
            self.event_queue.put({"type": self.EVENT_LOAD, "data": {"invoke_time": invoke_time}})
            self.event_empty_cond.notify_all()

    def _process_events(self):
        """Processes events from the event queue."""
        while True:
            event = None
            with self.lock:
                while self.event_queue.empty():
                    if self.closed:
                        self._flush_unloaded_uris()
                        logging.info("OSSWriter service stopped")
                        return
                    self.event_empty_cond.wait()

                event = self.event_queue.get()
                if self.event_queue.full():
                    self.event_full_cond.notify_all()

            if event["type"] == self.EVENT_ADD_SOURCE:
                self._handle_add_source(event["data"])
            elif event["type"] == self.EVENT_LOAD:
                self._handle_load(event["data"])

    def _handle_add_source(self, data):
        """Handles add_source event."""
        uri = data["uri"]
        self.unloaded_uris.append(uri)
        logging.info(f"Added source URI: {uri}")

    def _handle_load(self, data):
        """Handles load event."""
        invoke_time = data["invoke_time"]
        if self.unloaded_uris:
            table_name = self._round_up_15_minutes(invoke_time - timedelta(minutes=20)).strftime("%Y%m%d_%H%M")
            uris_to_load = self.unloaded_uris[:]
            self.unloaded_uris.clear()

            try:
                self._load_to_oss(table_name, uris_to_load)
                logging.info(f"Successfully loaded table: {table_name} with URIs: {uris_to_load}")
            except Exception as e:
                logging.error(f"Error loading table: {table_name} - {str(e)}")

    def _load_to_oss(self, table_name, uris):
        """Loads data to OSS as a mock of BigQuery."""
        content = "\n".join(uris).encode("utf-8")
        filename = f"{table_name}.txt"
        self.oss_bucket.put_object(filename, content)
        logging.info(f"Data uploaded to OSS as {filename}")

    @staticmethod
    def _round_up_15_minutes(dt):
        """Rounds up a datetime to the nearest 15 minutes."""
        new_minute = (dt.minute // 15 + 1) * 15
        return dt.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=new_minute)
