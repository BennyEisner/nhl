import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db.schema import engine
from src.utils.logger import get_logger

logger = get_logger(__name__)

def _read_sql(sql: str, params: dict = None) -> pd.DataFrame:
    with engine.connect() as conn: 
        df = pd.read_sql(text(sql), conn, params=params)
    return df

def get_games() -> pd.DataFrame:
    sql = """
        SELECT
            game_id, 
            season,
            date,
            home_team,
            away_team,
            home_score,
            away_score,
            home_win,
            went_to_ot
        FROM games
        WHERE home_win IS NOT NULL
        ORDER BY date ASC
    """
    df = _read_sql(sql)
   
    df["date"] = pd.to_datetime(df["date"]).dt.date # date strings to Pyhton date objects
    df["home_win"]  = df["home_win"].astype(int) # ensure it is 0 or 1 ant not converted to float

    logger.info(f"Loaded {len(df)} completed games across {df['season'].nunique()} seasons")

    return df


def get_team_stats() -> pd.DataFrame:
    sql = """
        SELECT
            team, 
            season, 
            is_home, 
            goals_for,
            goals_against,
            shots_for,
            shots_against,
            cf_pct,
            xgf_pct, 
            hdcf,
            hdca,
            CAST(hdcf AS REAL) / NULLIF(hdcf + hdca, 0) AS hdcf_pct
        -- Cast to REAL allows for float division since SQLite defaults to integer divison
        FROM team_game_stats 
        ORDER BY season ASC, team ASC
    """
    df = _read_sql(sql)
   
    logger.info(f"Loaded team stats: {len(df)} rows ({df['season'].nunique()} seasons, {df['team'].nunique()} teams)")

    return df

def get_games_for_team(team: str) -> pd.DataFrame:
    sql = """
        SELECT 
            game_id,
            season,
            date,
            home_team,
            away_team,
            home_score,
            away_score,
            home_win,
            went_to_ot,
            'home' AS perspective,
            home_score AS goals_for,
            away_score AS goals_against,
            home_win AS team_win
        FROM games
        WHERE home_team = :team AND home_win IS NOT NULL

        UNION ALL

        SELECT
            game_id,
            season,
            date,
            home_team,
            away_team,
            home_score,
            away_score,
            home_win,
            went_to_ot,
            'away' AS perspective,
            away_score AS goals_for,
            home_score AS goals_against,
            (1- home_win) AS team_win
        FROM games
        WHERE away_team = :team AND home_win IS NOT NULL
        ORDER BY date ASC
    """

    df = _read_sql(sql, params={"team": team})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["team_win"] = df["team_win"].astype(int)

    return df

def get_all_teams() -> list[str]:
    sql = """
        SELECT DISTINCT home_team AS team FROM games
        UNION 
        SELECT DISTINCT away_team AS team FROM games
        ORDER BY team ASC
    """

    df = _read_sql(sql)
    return df["team"].tolist()



def get_consensus_odds() -> pd.DataFrame: 
    sql = """
        SELECT
            game_id, 
            AVG(home_implied_prob) AS home_implied_prob, 
            AVG(away_implied_prob) AS away_implied_prob, 
            COUNT(DISTINCT bookmaker) AS num_bookmakers
        FROM odds
        GROUP BY game_id
    """
    
    df = _read_sql(sql)

    if len(df)==0:
        logger.warning("No odds data found")
    else: 
        logger.info(f"Loaded consensus odds for {len(df)} games")
    
    return df

