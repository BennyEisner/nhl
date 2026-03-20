import os
from pathlib import Path

from dotenv import load_dotenv

# Dont overrise variables already set in shell enviornment 
load_dotenv(override=False)

PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "nhl.db"

# Database
DB_URL = f"sqlite:///{DB_PATH}"

# API Keys
THE_ODDS_API_KEY = os.getenv("THE_ODDS_API_KEY")
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY")


# Global variabels
ROLLING_WINDOW = 10
HISTORICAL_SEASONA = [
    "20192020",
    "20202021",
    "20212022",
    "20222023",
    "20232024",
    "20242025",
]

CURRENT_SEASON = "20252026"
