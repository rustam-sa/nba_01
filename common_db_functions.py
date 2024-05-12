import pandas as pd
from sqlalchemy import select
from sqlalchemy.sql import join
from models import Team, Player, Game, GameStats
from db_config import get_session, get_database_engine


def get_teams_from_db():
    session = get_session()
    teams = session.query(Team).all()
    session.close()
    return teams


def get_players_from_db():
    session = get_session()
    players = session.query(Player).all()
    session.close()
    return players


def get_games_from_db():
    session = get_session()
    games = session.query(Game).all()
    session.close()
    return games


def fetch_player_game_log(player_name):
    engine = get_database_engine()
    session = get_session()
    
    try:
        # Find the player by name
        player = session.query(Player).filter(Player.name == player_name).one_or_none()
        
        if not player:
            print(f"No player found with the name {player_name}.")
            return None

        # Build a SQL query to fetch the game stats and the corresponding game date
        # We join GameStats with Game on game_id to fetch the date of each game
        # and filter by season_type and player_id
        query = (
            select(
                GameStats,
                Game.date.label('game_date')  # This labels the column as 'game_date'
            )
            .select_from(
                join(GameStats, Game, GameStats.game_id == Game.id)
            )
            .where(GameStats.player_id == player.id)
            .where(Game.season_type == season_type_filter)  # Filter by season_type
        )

        # Use pandas to directly load SQL results into DataFrame
        df = pd.read_sql(query, engine)

        return df

    finally:
        session.close()


def get_players_filtered_by_team_playoff_participation(season, playoffs=False):# Assuming get_database_engine returns an engine
    session = get_session()
    season_type = "Regular Season" if not playoffs else "Playoffs"

    query = (
        session.query(Player)
        .join(Team, Player.team_id == Team.id)
        .join(Game, (Team.id == Game.home_team_id) | (Team.id == Game.away_team_id))
        .filter(Game.season_type == season_type)
        .filter(Game.season == season)
        .distinct()  # To avoid duplicate players if they played in multiple playoff games
    )

    filtered_players = query.all()
    session.close()
    return filtered_players