"""
Query Redis - Check the top engaged content
"""
import redis
from config.logger_config import get_logger

logger = get_logger(__name__)


def query_top_content(limit=10):
    """
    Get top engaged content from Redis
    
    Args:
        limit: Number of top content to return
    """
    
    # Connect to Redis
    client = redis.Redis(
        host='localhost',
        port=6379,
        db=0,
        decode_responses=True
    )
    
    logger.info("Querying Redis for top engaged content...")
    
    # Get top N content by engagement (highest first)
    # ZREVRANGE returns members in descending order by score
    top_content = client.zrevrange('top_content', 0, limit - 1, withscores=True)
    
    if not top_content:
        logger.warning("No data found in Redis!")
        return
    
    logger.info("=" * 60)
    logger.info(f"Top {limit} Most Engaged Content")
    logger.info("=" * 60)
    
    for rank, (content_id, engagement_seconds) in enumerate(top_content, 1):
        logger.info(f"{rank}. Content ID: {content_id} | Engagement: {engagement_seconds}s")
    
    logger.info("=" * 60)
    
    # Get total count
    total_count = client.zcard('top_content')
    logger.info(f"Total unique content in Redis: {total_count}")
    
    client.close()


if __name__ == "__main__":
    query_top_content(limit=10)