import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent.parent

DATA_DIR = BASE_DIR / "data"
TEMP_DIR = DATA_DIR / "temp"
PROCESSED_DIR = DATA_DIR / "processed"
STATE_DIR = DATA_DIR / "state"
STORAGE_DIR = DATA_DIR / "storage"
METADATA_DIR = DATA_DIR / "metadata"
LOGS_DIR = BASE_DIR / "video_scraper" / "logs"

MAX_VIDEO_DURATION_SECONDS = int(os.getenv("MAX_VIDEO_DURATION_SECONDS", "900"))
VIDEO_WIDTH = int(os.getenv("VIDEO_WIDTH", "256"))
VIDEO_HEIGHT = int(os.getenv("VIDEO_HEIGHT", "256"))

DOWNLOAD_DELAY_MIN = float(os.getenv("DOWNLOAD_DELAY_MIN", "2"))
DOWNLOAD_DELAY_MAX = float(os.getenv("DOWNLOAD_DELAY_MAX", "5"))

SEARCH_DELAY_MIN = float(os.getenv("SEARCH_DELAY_MIN", "1"))
SEARCH_DELAY_MAX = float(os.getenv("SEARCH_DELAY_MAX", "3"))
SEARCH_FETCH_LIMIT = int(os.getenv("SEARCH_FETCH_LIMIT", "50"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

BACKOFF_BASE_DELAY = float(os.getenv("BACKOFF_BASE_DELAY", "5"))
BACKOFF_MAX_DELAY = float(os.getenv("BACKOFF_MAX_DELAY", "300"))
BACKOFF_FACTOR = float(os.getenv("BACKOFF_FACTOR", "2"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

for directory in [DATA_DIR, TEMP_DIR, PROCESSED_DIR, STATE_DIR, LOGS_DIR, STORAGE_DIR, METADATA_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
