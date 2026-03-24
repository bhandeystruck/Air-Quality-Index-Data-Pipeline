import requests
import boto3
import json
import logging
import os
import sys
from datetime import datetime, UTC
from dotenv import load_dotenv
from botocore.config import Config
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# 1. Load Environment Variables
load_dotenv()

# 2. Advanced Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s')
logger = logging.getLogger("AirQualityIngest")

# --- HELPER FUNCTIONS ---

def validate_data(data):
    """
    Data validation check.
    Ensures the data is worth saving before we pay for storage/compute.
    """
    if not data.get('hourly'):
        raise ValueError("Validation Failed: 'hourly' key is missing or null.")
    
    sample_key = 'pm2_5'
    if sample_key in data['hourly'] and len(data['hourly'][sample_key]) == 0:
        raise ValueError(f"Validation Failed: Hourly array for {sample_key} is empty.")

    logger.info("Data validation passed: Payload is structurally sound.")
    return True

# MOVED: The @retry decorator must be placed right above the function it is retrying
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    before_sleep=lambda retry_state: logger.warning(f"Retrying API call... Attempt {retry_state.attempt_number}")
)
def fetch_api_data(url, params):
    """Encapsulated API call with retry logic."""
    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    return response.json()

# --- MAIN LOGIC ---

def run_extraction():
    # Configuration from .env
    API_URL = os.getenv("AQ_API_URL")
    BUCKET = os.getenv("MINIO_BUCKET")
    
    params = {
        "latitude": os.getenv("LATITUDE"),
        "longitude": os.getenv("LONGITUDE"),
        "hourly": "pm10,pm2_5,nitrogen_dioxide",
        "timezone": "UTC"
    }

    try:
        # Step A: Extract
        raw_json = fetch_api_data(API_URL, params)

        # Step B: Validate 
        validate_data(raw_json)
        
        # Step C: Enrich
        raw_json['extraction_metadata'] = {
            "extracted_at": datetime.now(UTC).isoformat(),
            "retry_enabled": True,
            "python_standard": "3.12+",
            "record_count": len(raw_json['hourly'].get('time', []))
        }

        # Step D: Load to MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv("MINIO_ENDPOINT"),
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
            config=Config(retries={'max_attempts': 3})
        )

        date_partition = datetime.now().strftime("%Y-%m-%d")
        file_key = f"air_quality/load_date={date_partition}/aq_{datetime.now().strftime('%H%M%S')}.json"

        s3_client.put_object(
            Bucket=BUCKET,
            Key=file_key,
            Body=json.dumps(raw_json)
        )
        logger.info(f"Extraction Successful. File landed: {file_key}")

    except Exception as e:
        # Consolidated error handling for all pipeline steps
        logger.error(f"Critical Pipeline Failure: {e}")
        sys.exit(1) 

if __name__ == "__main__":
    run_extraction()