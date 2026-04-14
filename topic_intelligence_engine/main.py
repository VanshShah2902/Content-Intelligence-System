from core.database import Base, engine
from db.models import Post, PostMetrics

def main():
    print("Initializing Database...")
    try:
        # Create all tables defined in models.py
        Base.metadata.create_all(bind=engine)
        print("Database initialization successful!")
    except Exception as e:
        print(f"Error initializing database: {e}")

if __name__ == "__main__":
    main()
