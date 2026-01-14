import uuid
from sqlalchemy import (
    Column,
    String,
    Integer,
    Text,
    TIMESTAMP,
    ForeignKey,
    BigInteger,
    CheckConstraint,
    JSON
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Content(Base):
    __tablename__ = "content"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    slug = Column(Text, unique=True, nullable=False)
    title = Column(Text, nullable=False)
    content_type = Column(
        Text,
        CheckConstraint("content_type IN ('podcast', 'newsletter', 'video')")
    )
    length_seconds = Column(Integer)
    publish_ts = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class EngagementEvent(Base):
    __tablename__ = "engagement_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    content_id = Column(UUID(as_uuid=True), ForeignKey("content.id"))
    user_id = Column(UUID(as_uuid=True))
    event_type = Column(
        Text,
        CheckConstraint("event_type IN ('play', 'pause', 'finish', 'click')")
    )
    event_ts = Column(TIMESTAMP(timezone=True), nullable=False)
    duration_ms = Column(Integer)
    device = Column(Text)
    raw_payload = Column(JSON)