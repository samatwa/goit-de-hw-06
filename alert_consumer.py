import json
from kafka import KafkaConsumer
from datetime import datetime
from kafka_config import kafka_config

# Define the name of the Kafka topic
MY_NAME = "Viacheslav"

# Configure Kafka consumer for both alert topics
consumer = KafkaConsumer(
    f'temperature_alerts_{MY_NAME}',
    f'humidity_alerts_{MY_NAME}',
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=f'alert_consumer_group_{MY_NAME}',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


def display_alerts():
    """Display alerts received from both topics"""
    print("Starting alert monitoring...")

    for message in consumer:
        topic = message.topic
        alert = message.value

        alert_type = "TEMPERATURE" if "temperature" in topic else "HUMIDITY"

        print("\n" + "=" * 50)
        print(f"{alert_type} ALERT RECEIVED:")
        print(f"Sensor ID: {alert['sensor_id']}")
        print(f"Timestamp: {alert['timestamp']}")

        if "temperature" in topic:
            print(f"Temperature: {alert['temperature']}°C")
        else:
            print(f"Humidity: {alert['humidity']}%")

        print(f"Message: {alert['message']}")
        print("=" * 50 + "\n")


if __name__ == "__main__":
    try:
        display_alerts()
    except KeyboardInterrupt:
        print("Stopping alert consumer")
        consumer.close()