from confluent_kafka import Consumer, KafkaError
import oss2
import setting

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

def store_message_in_oss(message, filename):
    """Store a message in OSS with a specified filename."""
    # Convert the message to bytes (OSS requires binary data for uploading)
    message_bytes = message.encode('utf-8')
    
    # Upload the message to OSS
    result = bucket.put_object(filename, message_bytes)
    if result.status == 200:
        print(f'Message stored in OSS as {filename}')
    else:
        print('Failed to store message in OSS')

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

    # Process and store the received message in OSS
    message_str = msg.value().decode('utf-8')
    filename = f"message_{msg.timestamp()[1]}.txt"  # Timestamp-based filename for uniqueness
    store_message_in_oss(message_str, filename)

c.close()
