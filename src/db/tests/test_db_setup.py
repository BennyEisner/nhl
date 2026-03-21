"""Test database setup and basic CRUD operations."""
from datetime import date, datetime

from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker

from src.db.schema import Base, Game, MarketOdds, Odds, TeamGameStats, engine


def test_database_connection():
    """Test conneciton to database"""
    try:
        connection = engine.connect()
        connection.close()
        print("Database connection successful")
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False


def test_tables_exist():
    """Test that all expected tables exist"""
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    expected_tables = ["games", "team_game_stats", "odds", "market_odds"]

    for table in expected_tables:
        if table in tables:
            print(f"Table '{table}' exists")
        else:
            print(f"Table '{table}' missing")
            return False

    return True


def test_insert_and_query():
    """Test inserting and querying data."""
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Game table
        test_game = Game(
            game_id="2024020001",
            season="20242025",
            date=date(2024, 10, 8),
            home_team="BOS",
            away_team="TOR",
            home_score=4,
            away_score=3,
            home_win=True,
            went_to_ot=False
        )
        session.add(test_game)
        session.commit()
        print("Inserted test game")

        queried_game = session.query(Game).filter_by(game_id="2024020001").first()
        if queried_game and queried_game.home_team == "BOS":
            print("Queried test game successfully")
        else:
            print("Failed to query test game")
            return False

        test_stats = TeamGameStats(
            game_id="2024020001",
            team="BOS",
            is_home=True,
            goals_for=4,
            goals_against=3,
            shots_for=32,
            shots_against=28,
            cf_pct=52.5,
            xgf_pct=55.2,
            hdcf=8.0,
            hdca=6.0
        )
        session.add(test_stats)
        session.commit()
        print("Inserted test team stats")

        test_odds = Odds(
            game_id="2024020001",
            bookmaker="FanDuel",
            home_ml_=-150,
            away_ml_=130,
            home_implied_prob=0.60,
            away_implied_prob=0.435,
            fetched_at=datetime.now()
        )
        session.add(test_odds)
        session.commit()
        print("Inserted test odds")

        test_market_odds = MarketOdds(
            game_id="2024020001",
            source="Kalshi",
            home_prob=0.58,
            away_prob=0.42,
            fetched_at=datetime.now()
        )
        session.add(test_market_odds)
        session.commit()
        print("Inserted test market odds")

        try:
            duplicate_game = Game(
                game_id="2024020001",
                season="20242025",
                date=date(2024, 10, 8),
                home_team="BOS",
                away_team="TOR"
            )
            session.add(duplicate_game)
            session.commit()
            print("Unique constraint not working (duplicate game inserted)")
            return False
        except Exception:
            session.rollback()
            print("Unique constraint working (duplicate game rejected)")

        # Cleanup test data
        session.query(MarketOdds).filter_by(game_id="2024020001").delete()
        session.query(Odds).filter_by(game_id="2024020001").delete()
        session.query(TeamGameStats).filter_by(game_id="2024020001").delete()
        session.query(Game).filter_by(game_id="2024020001").delete()
        session.commit()
        print("Cleaned up test data")

        return True

    except Exception as e:
        print(f"Insert/Query test failed: {e}")
        session.rollback()
        return False
    finally:
        session.close()


def test_schema_columns():
    inspector = inspect(engine)

    # Test Game table columns
    game_columns = [col["name"] for col in inspector.get_columns("games")]
    expected_game_cols = ["id", "game_id", "season", "date", "home_team",
                          "away_team", "home_score", "away_score", "home_win", "went_to_ot"]

    for col in expected_game_cols:
        if col in game_columns:
            print(f"ames.{col} exists")
        else:
            print(f"games.{col} missing")
            return False

    return True


def run_all_tests():
    """Run all database tests."""
    tests = [
        ("Database Connection", test_database_connection),
        ("Tables Exist", test_tables_exist),
        ("Schema Columns", test_schema_columns),
        ("Insert and Query", test_insert_and_query),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        result = test_func()
        results.append((test_name, result))

    print("\n=== Test Summary ===")
    all_passed = all(result for _, result in results)

    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{status}: {test_name}")

    if all_passed:
        print("\n All tests passed! Database is set up correctly.")
    else:
        print("\n Some tests failed. Check the output above.")

    return all_passed


if __name__ == "__main__":
    run_all_tests()
