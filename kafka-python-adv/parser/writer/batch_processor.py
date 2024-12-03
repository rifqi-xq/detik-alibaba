import time
import json
import logging
from threading import Timer
from collections import defaultdict
import oss2

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