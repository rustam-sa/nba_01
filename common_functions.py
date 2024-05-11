from models import Team
from db_config import get_session

def get_teams_from_db():
    session = get_session()
    teams = session.query(Team).all()
    session.close()
    return teams