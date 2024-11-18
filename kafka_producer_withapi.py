from confluent_kafka import Producer
import json
import setting

# Initializations
conf = setting.kafka_setting
producer = Producer(
    {
        "bootstrap.servers": conf["bootstrap_servers"],
        "sasl.mechanisms": "PLAIN",
        "ssl.ca.location": conf["ca_location"],
        "security.protocol": "SASL_SSL",
        "ssl.endpoint.identification.algorithm": "none",
        "sasl.username": conf["sasl_plain_username"],
        "sasl.password": conf["sasl_plain_password"],
    }
)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def send_to_kafka(data: dict):
    try:
        message = json.dumps(data).encode("utf-8")
        producer.produce(conf["topic_name"], message, callback=delivery_report)
        producer.poll(0)
        print(f"Sent data to Kafka: {data}")
    except Exception as e:
        print(f"Failed to send data to Kafka: {e}")
        raise e
