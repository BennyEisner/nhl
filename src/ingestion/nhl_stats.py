from datetime import datetime

from nhlpy import NHLClient
from sqlalchemy.orm import Session

from src.db.schema import Game, engine
from src.utils.config import CURRENT_SEASON, HISTORICAL_SEASONS
from src.utils.logger import get_logger

logger = get_logger(__name__)

# API distinguishes between preseaosn regular season and playoffers by gametypes 1,2,3 respectively 
REGULAR_SEASON = 2

# NHL API client
client = NHLClient()


def _get_team_abbrevs() -> list[str]:
    teams = client.teams.teams()
    abbrevs = [t["abbr"] for t in teams]
    logger.info(f"Found {len(abbrevs)} teams")
    return abbrevs


# Mpa raw API game dictory to my Game Object
def _map_game(raw: dict) -> Game: 
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
        season = str(raw["season"]),
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
    team_abbrevs = _get_team_abbrevs()
    games_by_id: dict[str, Game] = {}
    for abbrev in team_abbrevs:
        logger.info(f"Fetching schedule: {abbrev} {season}")
        try: response = client.schedule.team_season_schedule(team_abbr = abbrev, season = season)
        except Exception as e: 
            logger.warning(f" Could not fetch schedule for {abbrev}: {e}")
            continue

        raw_games = response.get("games", [])

        for raw in raw_games: 
            # non regular season games
            if raw.get("gameType") != REGULAR_SEASON:
                continue
            # future games
            if raw.get("gameState") not in ("OFF", "FINAL"):
                continue
            game_id = str(raw["id"])

            if game_id not in games_by_id: 
                games_by_id[game_id] = _map_game(raw)

    for game in games_by_id.values():
        session.merge(game)
    

    logger.info(f"Season {season}: {len(games_by_id)} unique games merged") 


def ingest_nhl_stats():
    seasons = HISTORICAL_SEASONS + [CURRENT_SEASON]

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
