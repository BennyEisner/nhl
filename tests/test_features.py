from datetime import date

import pandas as pd

from src.db.queries import get_all_teams, get_games, get_games_for_team
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _compute_rolling_for_team(team_abbrev: str, window: int) -> pd.DataFrame:

    games = get_games_for_team(team_abbrev)

    rows = []

    for i, game in games.iterrows():
        game_date = game["date"]
        past_games = games[games["date"] < game_date]  # Ensure no leakage
        recent = past_games.tail(window)
        n_games = len(recent)

        # For first game of seaosn/db
        if n_games == 0:
            rows.append(
                {
                    "game_id": game["game_id"],
                    "team": team_abbrev,
                    "perspective": game["perspective"],
                    "rolling_win_pct": float("nan"),
                    "rolling_gf_per_game": float("nan"),
                    "rolling_ga_per_game": float("nan"),
                    "rolling_games_available": 0,
                }
            )

        else:
            rows.append(
                {
                    "game_id": game["game_id"],
                    "team": team_abbrev,
                    "perspective": game["perspective"],
                    "rolling_win_pct": recent["team_win"].mean(),
                    "rolling_gf_per_game": recent["goals_for"].mean(),
                    "rolling_ga_per_game": recent["goals_against"].mean(),
                    "rolling_games_available": n_games,
                }
            )

    return pd.DataFrame(rows)
