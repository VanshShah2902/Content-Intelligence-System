import sys
import os
from datetime import datetime, timezone

# Add project root to sys.path
project_root = r"C:\Users\vansh\OneDrive\Documents\zj\Content_intelligence_engine\topic_intelligence_engine"
if project_root not in sys.path:
    sys.path.append(project_root)

from core.database import SessionLocal
from ingestion.instagram import run_instagram_ingestion
from db.models import Post, PostMetrics

def verify_storage():
    print("Starting Step 2 Verification...")
    
    # Run ingestion
    print("Running Instagram Ingestion...")
    posts = run_instagram_ingestion()
    print(f"Ingestion returned {len(posts)} posts.")
    
    # Check database
    print("Checking Database...")
    db = SessionLocal()
    try:
        post_count = db.query(Post).count()
        metrics_count = db.query(PostMetrics).count()
        
        print(f"Total posts in DB: {post_count}")
        print(f"Total metrics in DB: {metrics_count}")
        
        if post_count > 0 and metrics_count > 0:
            print("[SUCCESS] Data successfully stored in PostgreSQL!")
        else:
            print("[FAILED] No data found in database.")
            
    except Exception as e:
        print(f"Error checking database: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    verify_storage()
