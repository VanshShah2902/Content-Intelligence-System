import os
import sys
import logging.config
import yaml

def setup_logger():
    # Setup basic logging configuration first
    os.makedirs('logs', exist_ok=True)
    
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'logging.yaml')
    
    config_loaded = False
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f.read())
            if config:
                logging.config.dictConfig(config)
                config_loaded = True
                
    if not config_loaded:
        # Fallback basic configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler("logs/topic_intelligence.log", encoding="utf-8")
            ]
        )
    return logging.getLogger("topic_intelligence")

logger = setup_logger()
