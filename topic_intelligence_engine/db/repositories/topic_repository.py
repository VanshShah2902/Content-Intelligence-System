from sqlalchemy.orm import Session
from db.models import Topic
from datetime import date

class TopicRepository:
    @staticmethod
    def get_by_name(db: Session, name: str):
        """Retrieve a topic by its name."""
        return db.query(Topic).filter(Topic.topic_name == name).first()

    @staticmethod
    def create(db: Session, topic_data: dict):
        """Create a new topic record."""
        db_topic = Topic(
            topic_name=topic_data.get("topic_name"),
            topic_signature=topic_data.get("topic_signature"),
            first_seen_date=topic_data.get("first_seen_date", date.today()),
            last_seen_date=topic_data.get("last_seen_date", date.today()),
            is_active=topic_data.get("is_active", True)
        )
        db.add(db_topic)
        db.commit()
        db.refresh(db_topic)
        return db_topic

    @staticmethod
    def update_last_seen(db: Session, topic_id, last_seen_date):
        """Update the last seen date for an existing topic."""
        db_topic = db.query(Topic).filter(Topic.topic_id == topic_id).first()
        if db_topic:
            db_topic.last_seen_date = last_seen_date
            db.commit()
            db.refresh(db_topic)
        return db_topic
