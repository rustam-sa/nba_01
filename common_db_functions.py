from models import Team, Player, Game
from db_config import get_session

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