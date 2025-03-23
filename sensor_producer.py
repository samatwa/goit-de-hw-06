import json
import time
import random
import uuid
from kafka import KafkaProducer
from datetime import datetime
from kafka_config import kafka_config

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define the name of the Kafka topic
MY_NAME = "Viacheslav"

# Generate a random sensor ID for this run
sensor_id = str(uuid.uuid4())[:8]


def generate_sensor_data():
    """Generate random temperature and humidity values"""
    temperature = round(random.uniform(25, 45), 1)
    humidity = round(random.uniform(15, 85), 1)
    timestamp = datetime.now().isoformat()

    return {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "temperature": temperature,
        "humidity": humidity
    }


def send_sensor_data():
    """Send sensor data to Kafka topic"""
    topic_name = f"building_sensors_{MY_NAME}"

    while True:
        data = generate_sensor_data()
        producer.send(topic_name, data)
        print(f"Sent: {data}")
        time.sleep(2)  # Send data every 2 seconds


if __name__ == "__main__":
    print(f"Starting sensor producer with ID: {sensor_id}")
    try:
        send_sensor_data()
    except KeyboardInterrupt:
        print("Stopping sensor producer")
        producer.close()