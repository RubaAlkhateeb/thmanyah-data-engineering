"""
Simple Redis Test - Check if Redis is working
"""
import redis
from config.logger_config import get_logger

logger = get_logger(__name__)


def test_redis_connection():
    """Test basic Redis connectivity"""
    try:
        # Connect to Redis
        logger.info("Step 1: Connecting to Redis...")
        client = redis.Redis(
            host='localhost',
            port=6379,
            db=0,
            decode_responses=True
        )
        
        # Test ping
        logger.info("Step 2: Testing ping...")
        response = client.ping()
        logger.info(f"✅ Redis responded: {response}")
        
        # Test set/get
        logger.info("Step 3: Testing set/get...")
        client.set('test_key', 'Hello Redis!')
        value = client.get('test_key')
        logger.info(f"✅ Set and retrieved: {value}")
        
        # Clean up
        client.delete('test_key')
        logger.info("✅ Cleanup done")
        
        logger.info("=" * 50)
        logger.info("✅ Redis test PASSED!")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Redis test FAILED: {e}")
        logger.error("Make sure Redis is running: docker-compose ps")
        raise


if __name__ == "__main__":
    test_redis_connection()