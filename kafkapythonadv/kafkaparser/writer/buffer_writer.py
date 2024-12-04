from datetime import datetime


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
