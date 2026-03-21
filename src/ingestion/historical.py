from datetime import date, datetime
from io import StringIO

import pandas as pd
import requests
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.schema import Game, TeamGameStats, engine
from src.utils.config import HISTORICAL_SEASONS
from src.utils.logger import get_logger

logger = get_logger(__name__)


# URL builder
# Moneypuck seaosn format different from NHL API 
def _build_url(season: str) -> str:
    year = season[:4]
    return (
        f"http://moneypuck.com/moneypuck/playerData/seasonSummary/"
        f"{year}/regular/teams.csv"
        # TODO: ...regular/teams/[lines or skaters or goalies]
    )

# Fetch raw CSV
def _fetch_csv(season: str) -> pd.DataFrame:
    url = _build_url(season)
    logger.info(f"Fetching MoneyPuck data for season {season} from {url}")

    # 403 with no headers so added header to avoid looking like a bot when fetching data
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status() # throws exception if HTTP status is 4xx or 5xx

    df = pd.read_csv(StringIO(response.text))
    logger.info(f"Fetched {len(df)} rows for season {season}")
    return df


# Map df row to a TeamGameStats Object
def _map_team_stats(row: pd.Series, season: str) -> TeamGameStats: 
    return TeamGameStats(
        game_id = f"{season}_{row['team']}",
        team = str(row["team"]), 
        season = season, 
        is_home = None, # not tracked by moneypuck
        goals_for = int(row["goalsFor"]),
        goals_against = int(row["goalsAgainst"]),
        shots_for = int(row["shotsOnGoalFor"]),
        shots_against = int(row["shotsOnGoalAgainst"]),
        cf_pct = float(row["corsiPercentage"]),
        xgf_pct = float(row["xGoalsPercentage"]),
        hdcf = float(row["highDangerShotsFor"]),
        hdca = float(row["highDangerShotsAgainst"]),
    )

# Insert rows with idempotency
def _insert_team_stats(df: pd.DataFrame, season: str, session: Session): 
    df_all = df[df["situation"] == "all"].copy()

    inserted = 0
    skipped = 0

    for _, row in df_all.iterrows():
        obj = _map_team_stats(row, season)
        with Session(engine)as session: 
            try: 
                session.merge(obj)
                session.commit()
                inserted += 1
            except IntegrityError: 
                # For when row already exists we just skip it
                session.rollback()
                skipped +=1

    logger.info(f"Merged {inserted} team stat rows for season {season}")


# Main entry point
def ingest_historical():
    for season in HISTORICAL_SEASONS: 
        try:
            df = _fetch_csv(season)
            with Session(engine) as session: 
                _insert_team_stats(df, season, session)
                session.commit()
            logger.info(f"Season {season} committed successfully")
        except requests.HTTPError as e: 
            logger.error(f"HTTP error fetching season {season}: {e}")
        except Exception as e: 
            logger.error(f"Unexpected error for season {season}: {e}")
            raise

if __name__ == "__main__":
    ingest_historical()





