from confluent_kafka import Producer
import setting
import time
import random
import json
from datetime import datetime

# Load Kafka configuration
conf = setting.kafka_setting

# Initialize a producer
p = Producer({'bootstrap.servers': conf['bootstrap_servers']})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def generate_sensor_data():
    """Simulates sensor data with a timestamp and random temperature."""
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(20.0, 30.0), 2)  # Random temperature between 20.0 and 30.0
    }

try:
    # Simulate continuous data streaming
    while True:
        # Generate a random sensor data message
        data = generate_sensor_data()
        
        # Convert the data to JSON format for structured messaging
        message = json.dumps(data).encode('utf-8')
        
        # Send the message to the specified Kafka topic
        p.produce(conf['topic_name'], message, callback=delivery_report)
        
        # Poll for any delivery reports (callbacks)
        p.poll(0)
        
        # Print the data for verification (optional)
        print(f"Sent data: {data}")
        
        # Wait for a short interval before sending the next message
        time.sleep(5)  # Send data every 5 seconds (adjust as needed for your use case)

except KeyboardInterrupt:
    # Graceful shutdown on interrupt
    print("Stopping data stream.")

finally:
    # Ensure all messages are sent before closing
    p.flush()
