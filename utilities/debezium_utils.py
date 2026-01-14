import requests
import json
import time
from config.logger_config import get_logger
from config.config import KAFKA_CONNECT_URL, DEBEZIUM_CONFIG

logger = get_logger(__name__)


def wait_for_kafka_connect():
    """Wait for Kafka Connect to be ready"""
    logger.info("Waiting for Kafka Connect to be ready...")
    
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get(f"{KAFKA_CONNECT_URL}/connectors")
            if response.status_code == 200:
                logger.info("Kafka Connect is ready!")
                return True
        except requests.exceptions.ConnectionError:
            pass
        
        logger.info(f"Waiting... ({i+1}/{max_retries})")
        time.sleep(2)
    
    logger.error("Kafka Connect did not start in time")
    return False


def create_connector():
    """Create Debezium PostgreSQL connector"""
    
    logger.info("Creating Debezium connector...")
    
    # Check if connector already exists
    response = requests.get(f"{KAFKA_CONNECT_URL}/connectors")
    existing_connectors = response.json()
    
    connector_name = DEBEZIUM_CONFIG["name"]
    
    if connector_name in existing_connectors:
        logger.info(f"Connector '{connector_name}' already exists. Deleting...")
        # Stop the connector first
        try:
            requests.put(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/stop")
            time.sleep(2)
        except:
            pass
        # Delete the connector
        requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}")
        time.sleep(2)
        logger.info("Connector deleted. Will create new one with fresh snapshot.")
    
    # Create connector
    response = requests.post(
        f"{KAFKA_CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(DEBEZIUM_CONFIG)
    )
    
    if response.status_code in [200, 201]:
        logger.info("Debezium connector created successfully!")
        logger.info(f"Topic will be: postgres.public.engagement_events")
        return True
    else:
        logger.error(f"Failed to create connector: {response.text}")
        return False


def check_connector_status():
    """Check connector status"""
    connector_name = DEBEZIUM_CONFIG["name"]
    
    response = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status")
    
    if response.status_code == 200:
        status = response.json()
        logger.info(f"Connector Status: {status['connector']['state']}")
        logger.info(f"Tasks: {len(status['tasks'])}")
        if status['tasks']:
            logger.info(f"Task State: {status['tasks'][0]['state']}")
    else:
        logger.error(f"Failed to get status: {response.text}")


def setup_debezium():
    """
    Simple setup function that can be imported
    Returns True if successful, False otherwise
    """
    logger.info("Setting up Debezium CDC...")
    
    # Wait for Kafka Connect
    if not wait_for_kafka_connect():
        return False
    
    # Create connector
    if not create_connector():
        return False
    
    # Wait a bit for connector to initialize
    time.sleep(3)
    
    # Check connector status
    check_connector_status()
    
    return True