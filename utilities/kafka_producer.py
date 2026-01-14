"""
Kafka Producer - Sends transformed events to Kafka
This module is called by main.py
"""
from utilities.kafka_utils import create_producer, send_message, create_topics
from config.logger_config import get_logger

logger = get_logger(__name__)


def send_events_to_kafka(transformed_data):
    """
    Send transformed events to Kafka
    
    Args:
        transformed_data: List of dicts (already transformed by apply_transformations)
    
    Returns:
        int: Number of events sent
    """
    
    # Step 1: Create Kafka topic
    logger.info("Creating Kafka topic...")
    create_topics(["engagement_events"])
    
    # Step 2: Check if we have data
    if not transformed_data or len(transformed_data) == 0:
        logger.warning("No data to send to Kafka!")
        return 0
    
    logger.info(f"Processing {len(transformed_data)} transformed events")
    
    # Step 3: Create Kafka producer
    logger.info("Creating Kafka producer...")
    producer = create_producer()
    
    # Step 4: Send each event to Kafka
    logger.info("Sending events to Kafka...")
    sent_count = 0
    
    for event in transformed_data:
        # Use content_id as key (events for same content go to same partition)
        key = str(event['content_id'])
        
        send_message(
            producer=producer,
            topic="engagement_events",
            message=event,
            key=key
        )
        
        sent_count += 1
        
        # Log progress every 100 events
        if sent_count % 100 == 0:
            logger.info(f"Sent {sent_count} events...")
    
    # Step 5: Close producer
    producer.close()
    
    logger.info(f"✅ Successfully sent {sent_count} events to Kafka!")
    return sent_count