import time
import json
import logging
from threading import Timer, Lock
from collections import defaultdict
from datetime import datetime


class BatchProcessor:
    def __init__(self, buffer_writer, batch_interval=60):
        """
        BatchProcessor integrates with BufferWriter for efficient message batching and flushing.
        Args:
            buffer_writer (BufferWriter): The BufferWriter instance for managing OSS writes.
            batch_interval (int): Interval in seconds to flush batches.
        """
        self.buffer_writer = buffer_writer
        self.message_buffer = defaultdict(list)
        self.batch_interval = batch_interval
        self.lock = Lock()

    def add_message_to_buffer(self, topic, message):
        """
        Adds a message to the in-memory buffer for the specified topic.
        Args:
            topic (str): Topic name.
            message (dict): Message payload.
        """
        with self.lock:
            self.message_buffer[topic].append(message)
            logging.info(f"Added message to topic {topic}: {message}")

    def store_batch(self):
        """
        Flushes the buffered messages for all topics to the BufferWriter.
        """
        with self.lock:
            current_time = datetime.utcnow()
            for topic, messages in list(self.message_buffer.items()):
                if not messages:
                    continue
                
                # Serialize messages to JSON
                batch_content = json.dumps(messages, indent=2).encode("utf-8")
                try:
                    # Add data to buffer
                    logging.info(f"Flushing batch for topic {topic} with {len(messages)} messages.")
                    self.buffer_writer.add_data(batch_content)
                except Exception as e:
                    logging.error(f"Error flushing batch for topic {topic}: {e}")
                
                # Clear messages for the topic
                self.message_buffer[topic].clear()

    def start_timer(self):
        """
        Starts the periodic batch flushing timer.
        """
        logging.info("Starting BatchProcessor timer...")
        self._schedule_next_flush()

    def _schedule_next_flush(self):
        """
        Schedules the next flush after the batch interval.
        """
        Timer(self.batch_interval, self._flush_and_reschedule).start()

    def _flush_and_reschedule(self):
        """
        Flushes current batches and schedules the next flush.
        """
        try:
            self.store_batch()
        except Exception as e:
            logging.error(f"Error during batch flush: {e}")
        finally:
            self._schedule_next_flush()
