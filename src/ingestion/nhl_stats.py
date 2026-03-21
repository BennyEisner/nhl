from datetime import datetime

from nhlpy import NHLClient
from sqlalchemy.orm import Session

from src.db.schema import Game, engine
from src.utils.config import CURRENT_SEASON, HISTORICAL_SEASONS
from src.utils.logger import get_logger

logger = get_logger(__name__)


# NHL API client
client = NHLClient()



# Mpa raw API game dictory to my Game Object
def _map_game(raw: dict, season: str) -> Game: 
    home = raw.get("homeTeam", {})
    away = raw.get("awayTeam", {})
    outcome = raw.get("gameOutcome", {})

    home_score = home.get("score")
    away_score = away.get("score")

    home_win = None
    if home_score is not None and away_score is not None: 
        home_win = home_score > away_score

    last_period = outcome.get("lastPeriodType", "REG")
    went_to_ot = last_period in ("OT", "SO")

    return Game(
        game_id = str(raw["id"]),
        season = season, 
        date = datetime.strptime(raw["gameDate"], "%Y-%m-%d").date(),
        home_team = home.get("abbrev"),
        away_team= away.get("abbrev"),
        home_score = home_score, 
        away_score = away_score,
        home_win = home_win, 
        went_to_ot = went_to_ot, 
    )

# Fetch and insert all games for a season
def _ingest_season(season: str, session: Session): 
    logger.info(f"Fetching NHL schedule for season {season}")
    schedule = client.schedule.get_season_schedule(team_abbr=None, season=season)

    games_raw = schedule if isinstance(schedule, list) else schedule.get("games", [])

    inserted = 0
    skipped = 0

    for raw in games_raw: 
        if raw.get("gameState") not in ("OFF", "FINAL"):
            skipped += 1
            continue
        
        game_obj = _map_game(raw, season)
        session.merge(game_obj)
        inserted += 1

    logger.info(f"Season {season}: {inserted} games merged, {skipped} future games skipped") 


def ingest_nhl_stats():
    seasons = HISTORICAL_SEASONS + [CURRENT_SEASON]
    seasons = list(dict.fromkeys(seasons)) # deduplicate inc ase current seaosn already in historical seasons

    for season in seasons: 
        try: 
            with Session(engine) as session: 
                _ingest_season(season, session)
                session.commit()
            logger.info(f"Season {season} committed successfully")

        except Exception as e: 
            logger.error(f"Error ingesting season {season}: {e}")
            raise

if __name__ == "__main__":
    ingest_nhl_stats()
