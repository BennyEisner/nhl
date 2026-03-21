from sqlalchemy import (Boolean, Column, Date, DateTime, Float, Integer,
                        String, UniqueConstraint, create_engine)
from sqlalchemy.orm import declarative_base

from src.utils.config import DB_URL

engine = create_engine(DB_URL, echo=False)

# Supercalss all table models inherit 
Base = declarative_base()

class Game(Base):
    __tablename__ = "games"

    game_id = Column(String, primary_key=True)

    season = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    home_team = Column(String, nullable=False)
    away_team = Column(String, nullable=False)
    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)
    home_win = Column(Boolean, nullable=True)
    went_to_ot = Column(Boolean, default=False)

    # prevent inserting same game twice
    __table_args__ = (UniqueConstraint("game_id", name="uq_game_id"),)



class TeamGameStats(Base): 
    __tablename__ = "team_game_stats"

    game_id = Column(String, primary_key=True)
    team = Column(String, primary_key = True)
    season = Column(String, nullable=False)
    is_home = Column(Boolean, nullable=True)

    goals_for = Column(Integer)
    goals_against = Column(Integer)
    shots_for = Column(Integer)
    shots_against = Column(Integer)
    
    cf_pct = Column(Float) # Corsi For % = shot attempts for / (for + against). Used for possession metric 
    xgf_pct = Column(Float) # Expected goals for
    hdcf = Column(Float) # High danger chances for 
    hdca = Column(Float) # High danger chanes against

    __table_args__ = (UniqueConstraint("game_id", "team", name="uq_game_team"),)


class Odds(Base):
    __tablename__ = "odds"

    game_id = Column(String, primary_key=True) 
    bookmaker = Column(String, primary_key=True)

    # TODO: specify and name money line(s) 
    home_ml = Column(Integer)
    away_ml = Column(Integer)

    home_implied_prob = Column(Float) # Convert american or other odds metrics to implied probability statistic
    away_implied_prob = Column(Float) 

    # Specific time on when specific line was fetched
    fetched_at = Column(DateTime, nullable=False)


# Prediction market odds Kalshi + Polymarket 
class MarketOdds(Base):
    
    __tablename__ = "market_odds"

    game_id = Column(String, primary_key=True)
    source = Column(String, primary_key=True)
    home_prob = Column(Float)
    away_prob = Column(Float)
    fetched_at = Column(DateTime, nullable=False)


# initialize tables 
def init_db():
    Base.metadata.create_all(engine)

if __name__ == "__main__": 
    init_db()
    print(f"Database initialized at: {DB_URL}")




    

