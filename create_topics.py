from kafka.admin import KafkaAdminClient, NewTopic
from kafka_config import kafka_config

# Configure Kafka admin client
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Define the name of the Kafka topic
MY_NAME = "Viacheslav"

# Create topic configurations
topic_list = [
    NewTopic(
        name=f"building_sensors_{MY_NAME}",
        num_partitions=1,
        replication_factor=1
    ),
    NewTopic(
        name=f"temperature_alerts_{MY_NAME}",
        num_partitions=1,
        replication_factor=1
    ),
    NewTopic(
        name=f"humidity_alerts_{MY_NAME}",
        num_partitions=1,
        replication_factor=1
    )
]

# Create topics and list them
try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("Topics created successfully:")
    for topic in topic_list:
        print(f"- {topic.name}")

    # List all topics that contain your name
    print("\nListing all topics containing your name:")
    [print(topic) for topic in admin_client.list_topics() if MY_NAME.lower() in topic.lower()]
except Exception as e:
    print(f"Error creating topics: {e}")
finally:
    admin_client.close()