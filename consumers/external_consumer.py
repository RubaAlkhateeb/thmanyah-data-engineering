import os
import requests
from datetime import datetime
from utilities.kafka_utils import create_consumer
from config.logger_config import get_logger
from config.config import EXTERNAL_API_URL, EXTERNAL_API_TOKEN, REQUEST_TIMEOUT_SECONDS, MAX_RETRIES

logger = get_logger(__name__)


def send_to_external_api(events, retry_count=0):
    """
    Send events to external API via HTTP POST
    
    Args:
        events: List of event dicts
        retry_count: Current retry attempt
    
    Returns:
        bool: True if successful, False otherwise
    """
    status_code = None
    if not events:
        return True, status_code
    
    try:
        # Prepare request
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Kafka-External-Consumer/1.0'
        }
        
        # Add authentication if token provided
        if EXTERNAL_API_TOKEN:
            headers['Authorization'] = f'Bearer {EXTERNAL_API_TOKEN}'
        
        # Prepare payload
        payload = {
            'events': events,
            'count': len(events),
            'timestamp': datetime.now().isoformat(),
            'source': 'kafka-streaming-pipeline'
        }
        
        # Send POST request
        response = requests.post(
            EXTERNAL_API_URL,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS
        )

        status_code = response.status_code
        
        # Check response
        if response.status_code in [200, 201, 202]:
            logger.debug(f"Sent {len(events)} events to external API (status: {response.status_code})")
            return True, status_code
        else:
            logger.warning(f"External API returned status {response.status_code}: {response.text[:200]}")
            
            # Retry on server errors (5xx)
            if response.status_code >= 500 and retry_count < MAX_RETRIES:
                logger.info(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRIES})")
                return send_to_external_api(events, retry_count + 1), status_code
            
            return False, status_code
    
    except requests.exceptions.Timeout:
        logger.error(f"Request timeout after {REQUEST_TIMEOUT_SECONDS}s")
        if retry_count < MAX_RETRIES:
            logger.info(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRIES})")
            return send_to_external_api(events, retry_count + 1), status_code
        return False, status_code
    
    except requests.exceptions.ConnectionError:
        logger.error("Connection error - external API unreachable")
        if retry_count < MAX_RETRIES:
            logger.info(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRIES})")
            return send_to_external_api(events, retry_count + 1), status_code
        return False, status_code
    
    except Exception as e:
        logger.error(f"Failed to send to external API: {e}")
        return False, status_code


def prepare_event(event):
    """
    Transform event for external API format
    
    You can customize this based on what the external system expects
    
    Args:
        event: Event dict from Kafka
    
    Returns:
        Dict in format expected by external API
    """
    return {
        'event_id': event.get('event_id'),
        'content_id': event.get('content_id'),
        'user_id': event.get('user_id'),
        'event_type': event.get('event_type'),
        'event_ts': event.get('event_ts'),
        'duration_ms': event.get('duration_ms'),
        'device': event.get('device'),
        'raw_payload': event.get('raw_payload'),
        'content_type': event.get('content_type'),
        'length_seconds': event.get('length_seconds'),
        'engagement_seconds': event.get('engagement_seconds'),
        'engagement_pct': event.get('engagement_pct')
    }


def consume_and_send_to_external():
    """
    Main consumer loop:
    1. Read from Kafka transformed topic
    2. Batch events
    3. Send to external API via HTTP POST
    """
    
    logger.info("Starting External System Consumer...")
    
    # Validate configuration
    if not EXTERNAL_API_URL:
        logger.warning("EXTERNAL_API_URL not configured!")
        logger.warning("Using default webhook.site URL (for testing only)")
        logger.warning("Set EXTERNAL_API_URL environment variable for production")
    
    # Create Kafka consumer
    consumer = create_consumer(
        topic="engagement_events_transformed",
        group_id="external_consumer_group"
    )
    
    logger.info("Consuming from: engagement_events_transformed")
    logger.info("Press Ctrl+C to stop")
    
    event_count = 0
    sent_count = 0
    failed_count = 0
    
    try:
        for message in consumer:
            event = message.value
            
            # Prepare event for external API
            external_event = prepare_event(event)
            success, status_code = send_to_external_api([external_event])

            if status_code == 429:
                logger.error("Received 429 (rate limited). Stopping external consumer now.")
                break

            if success:
                sent_count += 1
            else:
                failed_count += 1
            
            # Log progress
            if event_count % 100 == 0:
                success_rate = (sent_count / event_count * 100) if event_count > 0 else 0
                logger.info(f"Processed {event_count} events | Sent: {sent_count} | Failed: {failed_count} | Success rate: {success_rate:.1f}%")
    
    except KeyboardInterrupt:
        logger.info("External consumer stopped by user")
    
    finally:
        
        consumer.close()
        
        # Final statistics
        total_events = sent_count + failed_count
        success_rate = (sent_count / total_events * 100) if total_events > 0 else 0
        
        logger.info("External System Consumer Summary")
        logger.info(f"Total processed: {event_count}")
        logger.info(f"Successfully sent: {sent_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info(f"Success rate: {success_rate:.1f}%")
