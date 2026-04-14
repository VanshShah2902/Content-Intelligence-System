import os
import json
import requests
from dotenv import load_dotenv
from core.logger import logger

import time

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not found. Check your .env file or environment variables.")

# OPTIONAL DEBUG (REMOVE AFTER TESTING)
print(f"GROQ_API_KEY LOADED: {GROQ_API_KEY[:5]}...")

LLM_API_URL = "https://api.groq.com/openai/v1/chat/completions"
LLM_MODEL = "llama-3.1-8b-instant"

headers = {
    "Authorization": f"Bearer {GROQ_API_KEY}",
    "Content-Type": "application/json"
}

def call_llm_json(prompt: str, max_retries: int = 3) -> dict:
    for attempt in range(max_retries):
        try:
            response = requests.post(
                LLM_API_URL,
                json={
                    "model": LLM_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.0
                },
                headers=headers,
                timeout=30
            )
            
            # Special handling for Rate Limiting (429)
            if response.status_code == 429:
                wait_time = 2 ** (attempt + 1)
                logger.warning(f"Groq Rate Limit (429). Retrying in {wait_time}s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            data = response.json()
            raw_text = data["choices"][0]["message"]["content"]

            try:
                return json.loads(raw_text)
            except Exception:
                import re
                match = re.search(r'\{.*\}', raw_text, re.DOTALL)
                if match:
                    return json.loads(match.group())
                else:
                    raise ValueError(f"Invalid JSON from LLM: {raw_text}")

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                logger.error(f"Groq API Call failed after {max_retries} attempts: {str(e)}")
                raise Exception("LLM FAILED")
            
            wait_time = 2 ** (attempt + 1)
            logger.warning(f"Network error: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)

    raise Exception("LLM FAILED")
