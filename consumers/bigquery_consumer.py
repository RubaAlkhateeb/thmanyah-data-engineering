import os
import tempfile
import json
from google.cloud import bigquery
from google.api_core import retry
from datetime import datetime
from utilities.kafka_utils import create_consumer
from config.logger_config import get_logger
from config.config import (
    BIGQUERY_PROJECT,
    BIGQUERY_DATASET,
    BIGQUERY_TABLE,
    MAX_BATCH_SIZE,
    MIN_BATCH_SIZE,
    POLL_TIMEOUT_MS,
    IDLE_FLUSH_SECONDS
)

logger = get_logger(__name__)


def create_bigquery_client():
    """
    Create BigQuery client
    
    Authentication:
    - Uses Application Default Credentials (ADC)
    - Or GOOGLE_APPLICATION_CREDENTIALS env var
    """
    try:
        client = bigquery.Client(project=BIGQUERY_PROJECT)
        logger.info(f"Connected to BigQuery project: {BIGQUERY_PROJECT}")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to BigQuery: {e}")
        raise


def prepare_row(event):
    """
    Transform Kafka event to BigQuery row format
    
    Args:
        event: Event dict from Kafka
    
    Returns:
        Dict matching BigQuery schema
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


def insert_data(client, rows):
    """
    Insert rows to BigQuery using Load Jobs
    
    Args:
        client: BigQuery client
        rows: List of row dicts
    
    Returns:
        Number of rows inserted successfully
    """
    if not rows:
        return 0
    
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    
    try:
        # Write rows to temporary JSONL file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as temp_file:
            for row in rows:
                json.dump(row, temp_file)
                temp_file.write('\n')
            temp_filename = temp_file.name
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            # autodetect=False
        )
        
        # Load data from file
        with open(temp_filename, 'rb') as source_file:
            load_job = client.load_table_from_file(
                source_file,
                table_id,
                job_config=job_config
            )
        
        # Wait for job to complete
        load_job.result()
        
        # Clean up temp file
        os.unlink(temp_filename)
        
        if load_job.errors:
            logger.error(f"BigQuery load job errors: {load_job.errors}")
            return 0
        else:
            logger.debug(f"Loaded {len(rows)} rows to BigQuery (job: {load_job.job_id})")
            return len(rows)
    
    except Exception as e:
        logger.error(f"Failed to load to BigQuery: {e}")
        # Clean up temp file if it exists
        try:
            if 'temp_filename' in locals():
                os.unlink(temp_filename)
        except:
            pass
        return 0


from datetime import datetime

def consume_and_write_to_bigquery():
    logger.info("Starting BigQuery Consumer...")

    bq_client = create_bigquery_client()

    consumer = create_consumer(
        topic="engagement_events_transformed",
        group_id="bigquery_consumer_group"
    )

    logger.info("Consuming from: engagement_events_transformed")

    batch = []
    event_count = 0
    inserted_count = 0

    last_message_time = datetime.now()

    try:
        while True:
            records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

            now = datetime.now()
            idle_seconds = (now - last_message_time).total_seconds()

            # If we got messages, append them to the batch
            if records:
                last_message_time = now

                for _, messages in records.items():
                    for msg in messages:
                        row = prepare_row(msg.value)
                        batch.append(row)
                        event_count += 1

            # Flush conditions
            should_flush_big = len(batch) >= MAX_BATCH_SIZE
            should_flush_idle = idle_seconds >= IDLE_FLUSH_SECONDS and len(batch) > 0
            should_flush_min = should_flush_idle and len(batch) >= MIN_BATCH_SIZE

            # If idle and batch is small, you can decide: flush anyway or keep waiting.
            # Option A (recommended): flush even if small after being idle
            should_flush = should_flush_big or should_flush_idle

            # Option B: ONLY flush on idle if batch >= MIN_BATCH_SIZE
            # should_flush = should_flush_big or should_flush_min

            if should_flush:
                logger.info(
                    f"Flushing {len(batch)} rows "
                    f"(idle={idle_seconds:.1f}s, size={len(batch)})"
                )

                inserted = insert_data(bq_client, batch)

                if inserted == 0:
                    logger.error("Flush finished but inserted=0 (check insert_data logs/errors).")

                inserted_count += inserted
                batch.clear()

            if event_count > 0 and event_count % 100 == 0:
                logger.info(f"Processed {event_count} events | Inserted {inserted_count} rows")

    except KeyboardInterrupt:
        logger.info("BigQuery consumer stopped by user")

    finally:
        if batch:
            logger.info(f"Final flush of {len(batch)} rows")
            inserted = insert_data(bq_client, batch)
            inserted_count += inserted

        consumer.close()
        logger.info(f"Consumer closed. Total processed={event_count}, inserted={inserted_count}")