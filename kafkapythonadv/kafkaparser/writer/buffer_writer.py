import threading
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class BufferWriter:
    def __init__(self, oss_writer, base_path, max_size=10 * 1024 * 1024):
        """
        A writer that buffers data before sending it to an OSSWriter.

        Parameters:
        - oss_writer: An initialized OSSWriter instance.
        - base_path: The base path for storing objects in OSS.
        - max_size: Maximum buffer size in bytes before flushing.
        """
        self.oss_writer = oss_writer
        self.base_path = base_path
        self.max_size = max_size
        self.buffer = bytearray()
        self.seq = 0
        self.lock = threading.Lock()  # Ensures thread-safe operations

    def _new_object_name(self):
        """Generate a new OSS object name based on time and sequence."""
        timestamp = datetime.utcnow()
        window_path = timestamp.strftime("%Y/%m/%d/%H%M")
        object_name = f"{self.base_path}/{window_path}/data-{self.seq:020d}.log"
        self.seq += 1
        return object_name

    def flush(self):
        """Flush the current buffer to OSS."""
        with self.lock:
            if not self.buffer:
                return  # No data to flush

            object_name = self._new_object_name()
            try:
                # Delegate actual data upload to OSSWriter
                self.oss_writer.add_source_data(object_name)
                self.oss_writer.load()  # Explicitly trigger loading to OSS
                logging.info(f"Buffer flushed to OSS as {object_name}")
            except Exception as e:
                logging.error(f"Failed to flush buffer to OSS: {e}")
                # Optionally: implement retry logic or propagate the error
            finally:
                self.buffer.clear()

    def add_data(self, data):
        """Add data to the buffer and flush if needed."""
        with self.lock:
            if len(self.buffer) + len(data) > self.max_size:
                self.flush()
            self.buffer.extend(data)

    def close(self):
        """Flush any remaining data and close the writer."""
        with self.lock:
            self.flush()
            logging.info("BufferWriter closed.")
