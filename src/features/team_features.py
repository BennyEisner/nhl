import pandas as pd

from src.db.queries import get_games, get_team_stats
from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_team_season_features() -> pd.DataFrame:
    games = get_games()
    stats = get_team_stats()

    home_counts = (
        games.groupby(["season", "home_team"])
        .size()
        .reset_index(name="home_games")
        .rename(columns={"home_team": "team"})
    )

    away_counts = (
        games.groupby(["season", "away_team"])
        .size()
        .reset_index(name="away_games")
        .rename(columns={"away_team": "team"})
    )
    game_counts = pd.merge(
        home_counts, away_counts, on=["season", "team"], how="outer"
    ).fillna(0)

    game_counts["games_played"] = game_counts["home_games"] + game_counts["away_games"]

    stats = pd.merge(
        stats,
        game_counts[["season", "team", "games_played"]],
        on=["season", "team"],
        how="left",
    )

    stats["gf_per_game"] = stats["goals_for"] / stats["games_played"].replace(
        0, float("nan")
    )
    stats["ga_per_game"] = stats["goals_against"] / stats["games_played"].replace(
        0, float("nan")
    )

    logger.info(f"Computed per-game rates for {len(stats)} team-season rows")

    # Rename kept feature columns
    feature_cols = [
        "team",
        "season",
        "cf_pct",
        "xgf_pct",
        "hdcf_pct",
        "gf_per_game",
        "ga_per_game",
    ]

    stats_slim = stats[feature_cols].copy()

    # replace old column names to specify home_team
    away_stats = stats_slim.copy()
    away_stats.columns = [
        f"away_{c}" if c not in ("team", "season") else c for c in away_stats.columns
    ]
    away_stats = away_stats.rename(columns={"team": "away_team"})

    home_stats = stats_slim.copy()
    home_stats.columns = [
        f"home_{c}" if c not in ("team", "season") else c for c in home_stats.columns
    ]
    home_stats = home_stats.rename(columns={"team": "home_team"})

    df = pd.merge(games, home_stats, on=["season", "home_team"], how="left")
    df = pd.merge(df, away_stats, on=["season", "away_team"], how="left")

    df["cf_pct_diff"] = df["home_cf_pct"] - df["away_cf_pct"]
    df["xgf_pct_diff"] = df["home_xgf_pct"] - df["away_xgf_pct"]
    df["hdcf_pct_diff"] = df["home_hdcf_pct"] - df["away_hdcf_pct"]
    df["gf_per_game_diff"] = df["home_gf_per_game"] - df["away_gf_per_game"]
    df["ga_per_game_diff"] = df["home_ga_per_game"] - df["away_ga_per_game"]

    feature_columns = get_season_feature_columns()
    null_counts = df[feature_columns[2:]].isnull().sum()
    if null_counts.any():
        logger.warning(
            f"Null values found in features :\{null_counts[null_counts > 0]}"
        )
    else:
        logger.info("No null values in team season features")

    logger.info(
        f"Built team season features: {len(df)} rows, {len(df.columns)} columns"
    )

    return df


def get_season_feature_columns() -> list[str]:
    return [
        "home_cf_pct",
        "home_xgf_pct",
        "home_hdcf_pct",
        "home_gf_per_game",
        "home_ga_per_game",
        "away_cf_pct",
        "away_xgf_pct",
        "away_hdcf_pct",
        "away_gf_per_game",
        "away_ga_per_game",
        "cf_pct_diff",
        "xgf_pct_diff",
        "hdcf_pct_diff",
        "gf_per_game_diff",
        "ga_per_game_diff",
    ]
