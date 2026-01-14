import csv
import json
from pathlib import Path
import threading
import time
import requests
import traceback

from sqlalchemy import inspect, text, func
from sqlalchemy.orm import Session

from utilities.db_utils import get_db_engine, create_tables, seed_from_csv_if_empty
from utilities.debezium_utils import setup_debezium
from models.models import Base, Content, EngagementEvent
from config.logger_config import get_logger
from config.config import KAFKA_CONNECT_URL, DEBEZIUM_CONFIG

# Import streaming components
from utilities.transformer import run_transformer
from consumers.redis_consumer import consume_and_store
from consumers.bigquery_consumer import consume_and_write_to_bigquery
from consumers.external_consumer import consume_and_send_to_external

logger = get_logger(__name__)


def run_in_thread(func, name):
    """Run a function in a separate thread"""
    def wrapper():
        try:
            logger.info(f"Starting {name}...")
            func()
        except Exception as e:
            logger.error(f"{name} failed: {e}")
            logger.error(f"Full traceback:\n{traceback.format_exc()}")
    
    thread = threading.Thread(target=wrapper, name=name, daemon=True)
    thread.start()
    return thread


def main():
    """
    Hybrid approach:
    1. Initial setup (your original logic for database setup)
    2. Start CDC streaming (handles all data automatically)
    """
    logger.info("Starting Engagement Data Pipeline")
    
    # PART 1: Database setup only (your original main.py)
    logger.info("PART 1: Database Setup")
    
    # Setup database
    logger.info("Step 1: Setting up database...")
    engine = get_db_engine()
    create_tables(engine, {"content", "engagement_events"})
    
    # Configure tables to seed (automatic type conversion!)
    tables_config = [
        {'model': Content, 'csv_path': 'seed/content.csv'},
        {'model': EngagementEvent, 'csv_path': 'seed/engagement_events.csv'}
    ]
    
    seed_from_csv_if_empty(engine, tables_config)
    logger.info("Database setup complete")
    
    # PART 2: Start streaming components (CDC handles everything)
    logger.info("PART 2: Starting Real-Time CDC Pipeline")
    
    # Setup Debezium CDC (will snapshot existing data automatically)
    logger.info("Step 2: Setting up Debezium CDC...")
    if not setup_debezium():
        logger.error("Failed to setup Debezium. Exiting...")
        return
    
    # Wait for Debezium to process initial snapshot
    logger.info("Waiting for Debezium to capture existing data...")
    time.sleep(5)
    
    # Start streaming components
    logger.info("Step 3: Starting streaming components...")
    
    threads = []
    
    # Start Kafka Transformer (handles all transformations)
    transformer_thread = run_in_thread(run_transformer, "Kafka Transformer")
    threads.append(transformer_thread)
    
    # Start Redis Consumer
    redis_thread = run_in_thread(consume_and_store, "Redis Consumer")
    threads.append(redis_thread)
    
    # Start BigQuery Consumer
    bigquery_thread = run_in_thread(consume_and_write_to_bigquery, "BigQuery Consumer")
    threads.append(bigquery_thread)

    external_thread = run_in_thread(consume_and_send_to_external, "External API Consumer")
    threads.append(external_thread)
    
    logger.info("Pipeline fully operational!")
    logger.info("Running components:")
    logger.info("Debezium CDC (PostgreSQL → Kafka)")
    logger.info("Kafka Transformer (raw → transformed)")
    logger.info("Redis Consumer (time-window aggregations + latency tracking)")
    logger.info("BigQuery Consumer (transformed → BigQuery)")
    logger.info("External Consumer (transformed → Webhook API)")
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
            # Check if threads are still alive
            for thread in threads:
                if not thread.is_alive():
                    logger.warning(f"Thread {thread.name} died!")
    
    except KeyboardInterrupt:
        logger.info("")
        logger.info("Shutting down all components...")
        
        time.sleep(2)
        logger.info("All components stopped")


if __name__ == "__main__":
    main()