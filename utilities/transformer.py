import json
import time
from sqlalchemy.orm import Session
from utilities.kafka_utils import create_consumer, create_producer, send_message, create_topics
from utilities.db_utils import get_db_engine
from models.models import Content
from config.logger_config import get_logger
from config.config import KAFKA_BOOTSTRAP_SERVERS

logger = get_logger(__name__)


def extract_event_from_cdc(cdc_message):
    """
    Extract event payload from a Debezium CDC Kafka message.

    This function parses a Debezium change data capture (CDC) message and
    returns the relevant event data based on the operation type.

    Expected Debezium message structure:
        {
            "before": <json | null>,
            "after": <json | null>,
            "source": { ... },
            "op": "c" | "u" | "d" | "r",
            "ts_ms": <timestamp>,
            "transaction": <json | null>
        }
    Args:
        cdc_message (dict): Debezium CDC message consumed from a Kafka topic.

    Returns:
        dict | None: The extracted event payload (typically from "after"), or None
        if the message does not contain an event payload.
    """
    if not cdc_message:
        return None
    
    after = cdc_message.get('after') or cdc_message.get('payload', {}).get('after')
    
    if not after:
        logger.warning("No 'after' field in CDC message")
        return None
    
    return after


def transform_event(raw_event, session):
    """
    Apply transformations to event with content join
    
    Args:
        raw_event: Raw event from Kafka CDC topic
        session: SQLAlchemy session for querying content
    
    Returns:
        Transformed event with engagement_seconds, engagement_pct, content_type, length_seconds
    """

    content_id = raw_event.get('content_id')
    duration_ms = raw_event.get('duration_ms')
    
    # Query content table for content_type and length_seconds
    content_type = None
    length_seconds = None
    
    if content_id:
        try:
            result = session.query(Content.content_type, Content.length_seconds).filter(Content.id == content_id).first()
            if result:
                content_type, length_seconds = result
        except Exception as e:
            logger.warning(f"Failed to fetch content {content_id}: {e}")
    
    # Calculate engagement_seconds
    if duration_ms is not None:
        engagement_seconds = round(duration_ms / 1000.0, 2)
    else:
        engagement_seconds = None
    
    # Calculate engagement_pct
    if engagement_seconds is not None and length_seconds and length_seconds > 0:
        engagement_pct = round(engagement_seconds / length_seconds, 2)
    else:
        engagement_pct = None
    
    # Create transformed event
    transformed = {
        'event_id': raw_event.get('id'),
        'content_id': content_id,
        'user_id': raw_event.get('user_id'),
        'event_type': raw_event.get('event_type'),
        'event_ts': raw_event.get('event_ts'),
        'duration_ms': duration_ms,
        'device': raw_event.get('device'),
        'raw_payload': raw_event.get('raw_payload'),
        'content_type': content_type,
        'length_seconds': length_seconds,
        'engagement_seconds': engagement_seconds,
        'engagement_pct': engagement_pct
    }
    
    return transformed


def run_transformer():
    """
    Main transformer loop:
    1. Read from CDC topic
    2. Join with content data
    3. Transform
    4. Send to transformed topic
    """
    
    logger.info("Starting Transformer...")
    
    engine = get_db_engine()
    logger.info("Database Connection established")
    
    create_topics(["engagement_events_transformed"], bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    
    consumer = create_consumer(
        topic="postgres.public.engagement_events",
        group_id="transformer_group",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )
    
    # Create producer (sends to transformed topic)
    producer = create_producer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    
    logger.info("Transformer is running...")
    logger.info("Reading from: postgres.public.engagement_events")
    logger.info("Enriching with: content table (JOIN)")
    logger.info("Writing to: engagement_events_transformed")
    logger.info("Press Ctrl+C to stop")
    
    processed_count = 0
    
    try:
        logger.info("Waiting for messages from Kafka...")
        
        # Track when we receive first message
        start_time = time.time()
        
        for message in consumer:
            try:
                cdc_message = message.value
                
                # Log first message to debug
                if processed_count == 0:
                    elapsed = time.time() - start_time
                    logger.info(f"Received first message after {elapsed:.1f} seconds")
                    logger.info(f"First CDC message preview: {json.dumps(cdc_message, default=str)[:200]}...")
                
                # Extract actual event from CDC wrapper
                raw_event = extract_event_from_cdc(cdc_message)
                
                if not raw_event:
                    logger.debug("Skipping message - no 'after' field (likely a delete or tombstone)")
                    continue
                
                # Transform the event (with content enrichment)
                transformed_event = None
                with Session(engine) as session:
                    transformed_event = transform_event(raw_event, session)
                
                # Validate transformed event has required fields
                if not transformed_event or not transformed_event.get('content_id'):
                    logger.warning(f"Skipping event - missing content_id: {transformed_event.get('event_id') if transformed_event else 'unknown'}")
                    continue
                
                # Send to transformed topic
                send_message(
                    producer=producer,
                    topic="engagement_events_transformed",
                    message=transformed_event,
                    key=str(transformed_event['content_id'])
                )
                
                processed_count += 1
                
                # Log progress
                if processed_count % 100 == 0:
                    logger.info(f"Transformed {processed_count} events...")
                    
            except Exception as e:
                import traceback
                logger.error(f"Error processing message: {e}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                # Continue processing next message
                continue
    
    except KeyboardInterrupt:
        logger.info("Transformer stopped by user")
    except Exception as e:
        import traceback
        logger.error(f"Transformer encountered an error: {e}")
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        raise
    
    finally:
        consumer.close()
        producer.close()
        logger.info(f"Transformed total of {processed_count} events")
