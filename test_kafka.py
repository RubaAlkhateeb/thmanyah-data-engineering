"""
Simple Kafka Test Script
Tests that Kafka is working by:
1. Creating a test topic
2. Sending a message
3. Consuming the message
"""
from utilities.kafka_utils import create_producer, create_consumer, send_message, create_topics
from config.logger_config import get_logger

logger = get_logger(__name__)


def test_kafka():
    """Test basic Kafka functionality"""
    
    try:
        # Step 1: Create test topic
        logger.info("Step 1: Creating test topic...")
        create_topics(["test_topic"])
        
        # Step 2: Create producer and send message
        logger.info("Step 2: Sending test message...")
        producer = create_producer("localhost:9092")
        
        test_message = {
            "id": 1,
            "content": "Hello Kafka!",
            "timestamp": "2025-01-12T10:00:00"
        }
        
        send_message(producer, topic="test_topic", message=test_message, key="test_1")
        logger.info(f"✅ Sent message: {test_message}")
        
        producer.close()
        
        # Step 3: Create consumer and read message
        logger.info("Step 3: Consuming test message...")
        consumer = create_consumer(
            topic="test_topic",
            group_id="test_consumer_group",
            bootstrap_servers="localhost:9092"
        )
        
        # Read one message (with timeout)
        logger.info("Waiting for message...")
        for message in consumer:
            logger.info(f"✅ Received message: {message.value}")
            logger.info(f"   Topic: {message.topic}")
            logger.info(f"   Partition: {message.partition}")
            logger.info(f"   Offset: {message.offset}")
            logger.info(f"   Key: {message.key}")
            break  # Only read one message
        
        consumer.close()
        
        logger.info("=" * 50)
        logger.info("✅ Kafka test PASSED!")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Kafka test FAILED: {e}")
        logger.error("Make sure Kafka is running: docker-compose up -d")
        raise


if __name__ == "__main__":
    test_kafka()