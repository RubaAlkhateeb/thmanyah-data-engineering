import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
encoded_password = urllib.parse.quote_plus(DATABASE_PASSWORD)
DATABASE_HOST = os.getenv('DATABASE_HOST')
DATABASE_PORT = os.getenv('DATABASE_PORT', '5432')
DATABASE_NAME = os.getenv('DATABASE_NAME')

KAFKA_CONNECT_URL = os.getenv('KAFKA_CONNECT_URL', 'http://kafka-connect:8083')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
MAX_LATENCY_SAMPLES = 100
WINDOW_SIZE_MINUTES = 10
WINDOW_TTL_SECONDS = 3600  # Keep windows for 1 hour
CLEANUP_INTERVAL_SECONDS = 3600  # Run cleanup every 1 hour
LATENCY_WARN_TTL_SECONDS = 3600

BIGQUERY_PROJECT = os.getenv('BIGQUERY_PROJECT', 'thmanyah-de')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'engagement')
BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE', 'engagement_events')
MAX_BATCH_SIZE = 500
MIN_BATCH_SIZE = 50              # only flush early if at least this many
POLL_TIMEOUT_MS = 1000           # poll every 1s
IDLE_FLUSH_SECONDS = 10          # flush when no messages for 10s

EXTERNAL_API_URL = os.getenv('EXTERNAL_API_URL', 'https://webhook-test.com/0e4aa16f9464261e01b71819010890f0')
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3

DEBEZIUM_CONFIG = {
    "name": "postgres-engagement-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name":"pgoutput",
        "database.hostname": os.getenv('DEBEZIUM_CONFIG_DATABASE_HOSTNAME', 'postgres'),
        "database.port": DATABASE_PORT,
        "database.user": DATABASE_USER,
        "database.password": encoded_password,
        "database.dbname": DATABASE_NAME,
        "database.server.name": "postgres",
        "table.include.list": "public.engagement_events",
        "topic.prefix": "postgres",
        "slot.name": "debezium_slot",
        "publication.name": "debezium_publication",
        "snapshot.mode": "initial",
        "snapshot.locking.mode": "none"
    }
}