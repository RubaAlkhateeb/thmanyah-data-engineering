import os
import redis
import threading
import time
from datetime import datetime, timedelta, timezone
from utilities.kafka_utils import create_consumer
from config.logger_config import get_logger
from config.config import (
    REDIS_HOST,
    REDIS_PORT,
    KAFKA_BOOTSTRAP_SERVERS,
    WINDOW_SIZE_MINUTES,
    WINDOW_TTL_SECONDS,
    CLEANUP_INTERVAL_SECONDS,
    MAX_LATENCY_SAMPLES,
    LATENCY_WARN_TTL_SECONDS
)

logger = get_logger(__name__)

latency_samples = []

def create_redis_client():
    """Create Redis connection"""
    host = REDIS_HOST
    port = REDIS_PORT

    client = redis.Redis(
        host=host,
        port=port,
        db=0,
        decode_responses=True
    )
    client.ping()
    logger.info(f"Connected to Redis at {host}:{port}")
    return client


def get_window_key(timestamp):
    """
    Get the window key for a given timestamp
    
    Windows are aligned to clock (e.g., 12:00-12:09, 12:10-12:19)
    
    Args:
        timestamp: datetime object
    
    Returns:
        Window key string (e.g., "event_ts:2026-01-13T12:00")
    """
    # Round down to nearest 10-minute window
    minute = (timestamp.minute // WINDOW_SIZE_MINUTES) * WINDOW_SIZE_MINUTES
    window_time = timestamp.replace(minute=minute, second=0, microsecond=0)
    
    # Format as key: event_ts:YYYY-MM-DDTHH:MM
    return f"event_ts:{window_time.strftime('%Y-%m-%dT%H:%M')}"


def process_event(redis_client, event):
    """
    Process event and update aggregated counts in time window
    
    Also tracks latency: event_ts → processing_time
    
    Strategy:
    1. Determine which time window this event belongs to
    2. Increment the count for this content in that window (ZINCRBY)
    3. Set TTL on the window key for auto-cleanup
    4. Update the current window pointer
    5. Track latency for monitoring
    
    Redis structure per window:
        Key: event_ts:2026-01-13T12:00
        Type: Sorted Set
        Score: event count (aggregated)
        Member: content_id
    
    Args:
        redis_client: Redis connection
        event: Event data
    """
    try:
        content_id = event.get('content_id')
        event_ts = event.get('event_ts')
        
        if not content_id or not event_ts:
            logger.warning(f"Incomplete event data: {event}")
            return
        
        # Parse timestamp
        event_timestamp = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
        processing_timestamp = datetime.now(timezone.utc)
        
        # Calculate latency (event creation → Redis processing)
        latency_seconds = (processing_timestamp - event_timestamp).total_seconds()

        if latency_seconds > 5:
            warn_key = f"latency_warned:{event.get('event_id')}"
            
            # SETNX: returns True only if key did not exist
            first_time = redis_client.set(
                warn_key,
                "1",
                nx=True,
                ex=LATENCY_WARN_TTL_SECONDS
            )
            
            if first_time:
                logger.warning(
                    f"High latency on ingestion: event_id={event.get('event_id')} latency={latency_seconds:.2f}s"
                )
                
        # Get window key for this event's time
        window_key = get_window_key(event_timestamp)
        
        # Increment count for this content in this window
        # ZINCRBY: If member doesn't exist, creates it with score=1
        # If exists, adds 1 to existing score
        redis_client.zincrby(window_key, 1, content_id)
        
        # Set expiration on window (refresh TTL each time)
        redis_client.expire(window_key, WINDOW_TTL_SECONDS)
        
        # Update pointer to current window
        redis_client.set('event_ts:current', window_key)
        
        logger.debug(f"Updated window {window_key}: content={content_id}, latency={latency_seconds:.2f}s")
        
    except Exception as e:
        logger.error(f"Error processing event: {e}")


def get_top_content(redis_client, top_n=10):
    """
    Query top content from current time window
    
    Args:
        redis_client: Redis connection
        window_minutes: Must match WINDOW_SIZE_MINUTES
        top_n: Number of top content to return
    
    Returns:
        List of tuples: [(content_id, event_count), ...]
    """
    # Get current window key
    current_window = redis_client.get('event_ts:current')
    
    if not current_window:
        logger.warning("No current window found")
        return []
    
    # Get top N from sorted set (already sorted by score!)
    # ZREVRANGE: Get members with highest scores
    results = redis_client.zrevrange(
        current_window,
        0,
        top_n - 1,
        withscores=True
    )
    
    # Convert to list of tuples
    return [(content_id, int(score)) for content_id, score in results]


def consume_and_store():
    """
    Main consumer loop with time windowing
    
    Features:
    - Aggregates events into time windows
    - Latency monitoring (tracks < 5 second goal)
    """
    
    logger.info("Starting Redis Consumer (Time Window Mode)...")
    logger.info(f"Window size: {WINDOW_SIZE_MINUTES} minutes")
    logger.info(f"Window TTL: {WINDOW_TTL_SECONDS} seconds")
    logger.info(f"Cleanup interval: {CLEANUP_INTERVAL_SECONDS} seconds")
    
    # Connect to Redis
    redis_client = create_redis_client()
    
    # Create Kafka consumer
    consumer = create_consumer(
        topic="engagement_events_transformed",
        group_id="redis_consumer_group"
    )
    
    logger.info("Consuming from: engagement_events_transformed")
    logger.info("Aggregating events into time windows")
    logger.info("Press Ctrl+C to stop")
    
    event_count = 0
    
    try:
        for message in consumer:
            event = message.value
            
            # Process event (aggregates into time window + tracks latency)
            process_event(redis_client, event)
            
            event_count += 1
            
            # Log progress
            if event_count % 100 == 0:
                logger.info(f"Processed {event_count} events...")
                
                # Show top content
                top_content = get_top_content(redis_client, top_n=5)
                current_window = redis_client.get('event_ts:current')
                logger.info(f"Top 5 content in current window ({current_window}):")
                for i, (content_id, count) in enumerate(top_content, 1):
                    logger.info(f"  {i}. Content {content_id}: {count} events")
    
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    
    finally:
        consumer.close()
        redis_client.close()
        
        logger.info(f"Processed total of {event_count} events")
