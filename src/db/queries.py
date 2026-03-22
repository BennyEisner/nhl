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
            went_to_ot,
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
            cgf_pct, 
            hdcf,
            hdca
            CAST(hdcf AS REAL) / NULLIF(hdcf + hdca, 0) AS hdcf_pct
        -- Cast to REAL allows for float division since SQLite defaults to integer divison
        FROM team_game_stats 
        ORDER BY date ASC, team ASC
    """
    df = _read_sql(sql)
   
    logger.info(f"Loaded team stats: {len(df)} rows ({df['season'].nunique()} seasons, {df['team'].nunique()} teams)")

    return df

