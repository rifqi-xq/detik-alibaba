from confluent_kafka import Consumer, KafkaError
import oss2
import setting
import time
from threading import Timer
from collections import defaultdict

# Load Kafka and OSS configuration
kafka_conf = setting.kafka_setting
oss_conf = setting.oss_setting

# Initialize the Kafka consumer
c = Consumer({
    'bootstrap.servers': kafka_conf['bootstrap_servers'],
    'group.id': kafka_conf['group_name'],
    'auto.offset.reset': 'latest'
})

# Subscribe to multiple topics
c.subscribe([kafka_conf['topic_name']])

# Initialize the OSS connection
auth = oss2.Auth(oss_conf['oss_access_key_id'], oss_conf['oss_access_key_secret'])
bucket = oss2.Bucket(auth, oss_conf['oss_endpoint'], oss_conf['oss_bucket_name'])

# Initialize message buffer and timer
message_buffer = defaultdict(list)  # Store messages by topic
batch_interval = 15 * 60  # 15 minutes in seconds

def store_batch_in_oss():
    """Store all messages in the buffer as a batch in OSS."""
    current_time = int(time.time())
    for topic, messages in message_buffer.items():
        if messages:
            # Create a filename based on the topic and current time
            filename = f"{topic}_batch_{current_time}.txt"
            
            # Join all messages into a single string
            batch_content = "\n".join(messages).encode('utf-8')
            
            # Upload the batch to OSS
            result = bucket.put_object(filename, batch_content)
            if result.status == 200:
                print(f"Batch stored in OSS as {filename}")
            else:
                print(f"Failed to store batch for topic {topic} in OSS")
            
            # Clear the buffer for this topic after uploading
            message_buffer[topic].clear()
    
    # Restart the timer for the next batch
    start_batch_timer()

def start_batch_timer():
    """Start a timer to trigger batch storage every 15 minutes."""
    Timer(batch_interval, store_batch_in_oss).start()

# Start the timer for the first batch
start_batch_timer()

# Poll messages continuously
while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print("Consumer error: {}".format(msg.error()))
            continue

    # Decode message and add it to the appropriate buffer based on topic
    message_str = msg.value().decode('utf-8')
    topic = msg.topic()
    message_buffer[topic].append(message_str)

c.close()
