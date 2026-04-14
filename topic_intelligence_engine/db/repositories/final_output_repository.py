from sqlalchemy.orm import Session
from db.models import FinalOutput
from datetime import date

class FinalOutputRepository:
    @staticmethod
    def create(db: Session, output_data: dict):
        """Create a new final output record for strategic insights."""
        db_output = FinalOutput(
            topic_id=output_data.get("topic_id"),
            date=output_data.get("date", date.today()),
            intelligence_score=output_data.get("intelligence_score"),
            opportunity_score=output_data.get("opportunity_score"),
            content_saturation_score=output_data.get("content_saturation_score"),
            recommended_action=output_data.get("recommended_action"),
            competition_insight=output_data.get("competition_insight"),
            content_strategy=output_data.get("content_strategy"),
            target_audience=output_data.get("target_audience"),
            risk_factor=output_data.get("risk_factor"),
            angles=output_data.get("angles"),
            hooks=output_data.get("hooks"),
            key_points=output_data.get("key_points")
        )
        db.add(db_output)
        db.commit()
        db.refresh(db_output)
        return db_output
