import uuid
from datetime import datetime
from sqlalchemy import Column, String, Text, Integer, Numeric, Boolean, Date, DateTime, ForeignKey, Table, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from core.database import Base

# Association Table for CLUSTERS <-> POSTS Many-to-Many
cluster_posts = Table(
    "cluster_posts",
    Base.metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("cluster_id", UUID(as_uuid=True), ForeignKey("clusters.cluster_id", ondelete="CASCADE"), nullable=False),
    Column("post_id", String, ForeignKey("posts.post_id", ondelete="CASCADE"), nullable=False),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    UniqueConstraint("cluster_id", "post_id", name="uq_cluster_post")
)

class Post(Base):
    __tablename__ = "posts"

    post_id = Column(String, primary_key=True)
    creator_id = Column(String, nullable=False)

    caption = Column(Text)
    clean_text = Column(Text)

    post_url = Column(String, nullable=False, unique=True)
    image_url = Column(String, nullable=True)

    created_at = Column(DateTime)
    
    # Relationships
    metrics = relationship("PostMetrics", backref="post")
    clusters = relationship("Cluster", secondary=cluster_posts, back_populates="posts")

class PostMetrics(Base):
    __tablename__ = "post_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)

    post_id = Column(String, ForeignKey("posts.post_id"))
    captured_at = Column(DateTime, nullable=False)

    likes = Column(Integer)
    comments = Column(Integer)
    views = Column(Integer)

class Topic(Base):
    __tablename__ = "topics"

    topic_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    topic_name = Column(String(255), nullable=False)
    topic_signature = Column(JSONB, nullable=True)
    first_seen_date = Column(Date, nullable=False)
    last_seen_date = Column(Date, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    clusters = relationship("Cluster", back_populates="topic", cascade="all, delete-orphan")
    history = relationship("TopicHistory", back_populates="topic", cascade="all, delete-orphan")
    final_outputs = relationship("FinalOutput", back_populates="topic", cascade="all, delete-orphan")

class Cluster(Base):
    __tablename__ = "clusters"

    cluster_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    topic_id = Column(UUID(as_uuid=True), ForeignKey("topics.topic_id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    avg_momentum = Column(Numeric(10, 4), nullable=True)
    total_posts = Column(Integer, default=0)
    trend_stage = Column(String(50), nullable=True)
    content_saturation_score = Column(Numeric(10, 4), nullable=True)
    opportunity_score = Column(Numeric(10, 4), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    topic = relationship("Topic", back_populates="clusters")
    posts = relationship("Post", secondary=cluster_posts, back_populates="clusters")

class TopicHistory(Base):
    __tablename__ = "topic_history"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    topic_id = Column(UUID(as_uuid=True), ForeignKey("topics.topic_id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    momentum = Column(Numeric(10, 4), nullable=True)
    growth_rate = Column(Numeric(10, 4), nullable=True)
    trend_stage = Column(String(50), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("topic_id", "date", name="uq_topic_history_date"),
    )

    # Relationships
    topic = relationship("Topic", back_populates="history")

class Metric(Base):
    __tablename__ = "metrics"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_type = Column(String(50), nullable=False)  # 'post' or 'topic'
    entity_id = Column(UUID(as_uuid=True), nullable=False)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(Numeric(15, 4), nullable=False)
    recorded_at = Column(DateTime(timezone=True), server_default=func.now())

class FinalOutput(Base):
    __tablename__ = "final_outputs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    topic_id = Column(UUID(as_uuid=True), ForeignKey("topics.topic_id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    intelligence_score = Column(Numeric(10, 4), nullable=True)
    opportunity_score = Column(Numeric(10, 4), nullable=True)
    content_saturation_score = Column(Numeric(10, 4), nullable=True)
    recommended_action = Column(String(100), nullable=True)
    competition_insight = Column(Text, nullable=True)
    content_strategy = Column(String(100), nullable=True)
    target_audience = Column(Text, nullable=True)
    risk_factor = Column(String(50), nullable=True) 
    angles = Column(JSONB, nullable=True)
    hooks = Column(JSONB, nullable=True)
    key_points = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("topic_id", "date", name="uq_final_output_date"),
    )

    # Relationships
    topic = relationship("Topic", back_populates="final_outputs")
