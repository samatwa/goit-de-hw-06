import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka_config import kafka_config

# Define the name of the Kafka topic
MY_NAME = "Viacheslav"

# Configure Kafka consumer
consumer = KafkaConsumer(
    f'building_sensors_{MY_NAME}',
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=f'sensor_processor_group_{MY_NAME}',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Configure Kafka producer for alerts
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def process_sensor_data():
    """Process incoming sensor data and generate alerts when thresholds are exceeded"""
    print("Starting sensor data processing...")

    for message in consumer:
        data = message.value
        print(f"Processing: {data}")

        sensor_id = data['sensor_id']
        temperature = data['temperature']
        humidity = data['humidity']
        timestamp = data['timestamp']

        # Check temperature threshold
        if temperature > 40.0:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "message": f"HIGH TEMPERATURE ALERT: {temperature}°C exceeds threshold of 40°C"
            }
            producer.send(f'temperature_alerts_{MY_NAME}', alert)
            print(f"Temperature alert sent: {alert}")

        # Check humidity thresholds
        if humidity > 80.0 or humidity < 20.0:
            condition = "HIGH" if humidity > 80.0 else "LOW"
            threshold = "80%" if humidity > 80.0 else "20%"

            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "humidity": humidity,
                "message": f"{condition} HUMIDITY ALERT: {humidity}% {'exceeds' if humidity > 80.0 else 'below'} threshold of {threshold}"
            }
            producer.send(f'humidity_alerts_{MY_NAME}', alert)
            print(f"Humidity alert sent: {alert}")


if __name__ == "__main__":
    try:
        process_sensor_data()
    except KeyboardInterrupt:
        print("Stopping sensor data processor")
        consumer.close()
        producer.close()