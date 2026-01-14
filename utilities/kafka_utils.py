import json
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config.logger_config import get_logger
from config.config import KAFKA_BOOTSTRAP_SERVERS

logger = get_logger(__name__)


def json_serializer(data):
    """Convert dict to JSON bytes, handles datetime and UUID
    Args:
        data: dict to serialize
    """
    def helper(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)
    
    return json.dumps(data, default=helper).encode('utf-8')


def json_deserializer(data):
    """Convert JSON bytes to dict
    Args:
        data: JSON bytes
    """
    return json.loads(data.decode('utf-8'))


def create_producer(bootstrap_servers=None):
    """Create a Kafka producer
    Args:
        bootstrap_servers: Kafka bootstrap servers (optional)
    """
    if bootstrap_servers is None:
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=json_serializer,
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )
    logger.info(f"Created Kafka producer connected to {bootstrap_servers}")
    return producer


def send_message(producer, topic, message, key=None):
    """Send a message to Kafka
    Args:
        producer: Kafka producer instance
        topic: Kafka topic to send to
        message: Message dict to send
        key: Optional message key
    """
    try:
        future = producer.send(topic, value=message, key=key)
        # Wait for the message to be sent and get the result
        record_metadata = future.get(timeout=10)
        producer.flush()
        logger.debug(f"Sent message to topic '{topic}' partition {record_metadata.partition} offset {record_metadata.offset} with key '{key}'")
    except Exception as e:
        logger.error(f"Failed to send message to topic '{topic}': {e}")
        raise


def create_consumer(topic, group_id, bootstrap_servers=None):
    """Create a Kafka consumer
    Args:
        topic: Kafka topic to subscribe to
        group_id: Consumer group ID
        bootstrap_servers: Kafka bootstrap servers (optional)
    """
    if bootstrap_servers is None:
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=json_deserializer,
        auto_offset_reset="earliest"
    )
    logger.info(f"Created Kafka consumer for topic '{topic}' with group_id '{group_id}'")
    return consumer


def create_topics(topic_names, bootstrap_servers=None):
    """Create Kafka topics (idempotent).
    Args:
        topic_names: List of topic names to create
        bootstrap_servers: Kafka bootstrap servers (optional)
    """
    if bootstrap_servers is None:
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    try:
        existing = set(admin.list_topics())
        topics_to_create = [
            NewTopic(name=name, num_partitions=3, replication_factor=1)
            for name in topic_names
            if name not in existing
        ]

        if not topics_to_create:
            logger.info(f"Topics already exist: {topic_names}")
            return

        try:
            admin.create_topics(topics_to_create)
            logger.info(f"Created topics: {[t.name for t in topics_to_create]}")
        except TopicAlreadyExistsError:
            logger.info("Topic already exists (race). Continuing.")
    finally:
        admin.close()