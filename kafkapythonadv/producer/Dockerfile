FROM python:3.6.8

# Set the working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the consumer script and configurations
COPY kafka_producer.py .
COPY setting.py .

# Default command to run the Kafka consumer
CMD ["python", "kafka_producer.py"]
