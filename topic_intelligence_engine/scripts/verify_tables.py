import sys
import os

# Add project root to sys.path
project_root = r"C:\Users\vansh\OneDrive\Documents\zj\Content_intelligence_engine\topic_intelligence_engine"
if project_root not in sys.path:
    sys.path.append(project_root)

from sqlalchemy import inspect
from core.database import engine

def main():
    print("Verifying Tables...")
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(f"Tables found: {', '.join(tables)}")
        
        expected_tables = ["posts", "post_metrics"]
        for table in expected_tables:
            if table in tables:
                print(f"[SUCCESS] Table '{table}' exists.")
            else:
                print(f"[FAILED] Table '{table}' NOT found.")
                
    except Exception as e:
        print(f"Error connecting to database: {e}")

if __name__ == "__main__":
    main()
