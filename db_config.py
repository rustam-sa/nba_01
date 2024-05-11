import configparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_database_engine():
    config = configparser.ConfigParser()
    config.read('config.ini')
    database_url = config['database']['url']
    engine = create_engine(database_url)
    return engine

def get_session():
    engine = get_database_engine()
    Session = sessionmaker(engine)
    session = Session()
    return session
