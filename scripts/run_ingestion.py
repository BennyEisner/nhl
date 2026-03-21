import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.schema import init_db
from src.ingestion.historical import ingest_historical
from src.ingestion.nhl_stats import ingest_nhl_stats
from src.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Starting ingestion pipeline")

    logger.info("Initializing database...")
    init_db()

    logger.info("Ingesting Money Puck historical data...")
    ingest_historical()

    logger.info("Ingesting NHL game results from NHL API...")
    ingest_nhl_stats()

    logger.info("Ingestion done")

if __name__=="__main__":
    main()


