from sqlalchemy import create_engine, inspect, text, func, Integer, BigInteger, JSON
from sqlalchemy.orm import Session
import csv
import json

from config.config import DATABASE_USER, encoded_password, DATABASE_HOST, DATABASE_PORT, DATABASE_NAME
from config.logger_config import get_logger
from models.models import Base

logger = get_logger(__name__)


def get_db_engine():
    """Create and return a SQLAlchemy engine for the database."""
    connection_string = f"postgresql://{DATABASE_USER}:{encoded_password}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
    
    engine = create_engine(connection_string)
    
    return engine


def create_tables(engine, table_list):
    """
    Create tables in the database if they do not exist.
    Args:
        engine: SQLAlchemy engine
        table_list: Set of table names to ensure
    """
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())

    if table_list.issubset(existing_tables):
        return

    with engine.begin() as conn:
        conn.execute(text('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'))

    Base.metadata.create_all(bind=engine, checkfirst=True)
    logger.info("Tables ensured")


def seed_from_csv_if_empty(engine, tables_config):
    """
    Generic CSV seeding function with automatic type conversion
    
    Uses SQLAlchemy column types to automatically apply transformations:
    - Integer/BigInteger → int()
    - JSON → json.loads()
    - Others → use as-is
    
    Args:
        engine: SQLAlchemy engine
        tables_config: List of dicts with 'model' and 'csv_path'
    
    Example:
        tables_config = [
            {'model': Content, 'csv_path': 'seed/content.csv'},
            {'model': EngagementEvent, 'csv_path': 'seed/engagement_events.csv'}
        ]
    """
    with Session(engine) as session:
        logger.info("Loading seed data from CSV files...")
        
        any_data_exists = False
        
        for config in tables_config:
            model = config['model']
            csv_path = config['csv_path']
            table_name = model.__tablename__
            
            # Check if table has data
            count = session.query(func.count(model.id)).scalar()
            
            if count > 0:
                logger.info(f"Table '{table_name}' already has {count} rows. Skipping.")
                any_data_exists = True
                continue
            
            # Get column types from model
            column_types = {}
            for column in model.__table__.columns:
                column_types[column.name] = column.type
            
            # Load data from CSV
            try:
                with open(csv_path, newline="") as f:
                    reader = csv.DictReader(f)
                    objects = []
                    
                    for row in reader:
                        # Apply automatic transformations based on column types
                        transformed_data = {}
                        
                        for field, value in row.items():
                            if not value:
                                transformed_data[field] = None
                                continue

                            col_type = column_types.get(field)
                            
                            if col_type is None:
                                transformed_data[field] = value
                            elif isinstance(col_type, (Integer, BigInteger)):
                                transformed_data[field] = int(value)
                            elif isinstance(col_type, JSON):
                                transformed_data[field] = json.loads(value)
                            else:
                                transformed_data[field] = value
                        
                        # Create model instance
                        obj = model(**transformed_data)
                        objects.append(obj)
                
                if objects:
                    session.bulk_save_objects(objects)
                    session.flush()
                    logger.info(f"Inserted {len(objects)} rows into '{table_name}'")
                else:
                    logger.warning(f"No data found in {csv_path}")
            
            except FileNotFoundError:
                logger.error(f"CSV file not found: {csv_path}")
            except Exception as e:
                logger.error(f"Error loading {csv_path}: {e}")
        
        session.commit()
        
        if any_data_exists:
            logger.info("Database already contains data.")