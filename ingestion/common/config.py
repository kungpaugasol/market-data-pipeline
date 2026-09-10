# ingestion/common/config.py
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BRONZE_ROOT = Path(os.getenv("BRONZE_ROOT", "data/bronze"))
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL") or None
UNIVERSE_PATH = Path(os.getenv("UNIVERSE_PATH", "ingestion/universe/equities_universe.csv"))
