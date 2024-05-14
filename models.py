from sqlalchemy import Column, Integer, String, ForeignKey, Date, Float, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from db_config import get_database_engine

Base = declarative_base()
metadata = Base.metadata


class Team(Base):
    __tablename__ = 'teams'
    id = Column(Integer, primary_key=True)
    nba_team_id = Column(Integer, unique=True, nullable=False)
    nickname = Column(String)  
    city = Column(String)
    state = Column(String)
    full_name = Column(String)  
    abbreviation = Column(String)  
    players = relationship("Player", back_populates="team")

class Player(Base):
    __tablename__ = 'players'
    id = Column(Integer, primary_key=True)
    nba_player_id = Column(Integer, unique=True, nullable=False)
    name = Column(String)
    nickname = Column(String)
    player_slug = Column(String)
    jersey_number = Column(String)
    position = Column(String)
    height = Column(String)
    weight = Column(Float)
    birth_date = Column(Date)
    age = Column(Integer)
    experience = Column(String)
    school = Column(String)
    how_acquired = Column(String)
    team_id = Column(Integer, ForeignKey('teams.id'))
    team = relationship("Team", back_populates="players")
    stats = relationship("GameStats", back_populates="player")

class TeamStats(Base):
    __tablename__ = 'team_stats'
    id = Column(Integer, primary_key=True)
    game_id = Column(Integer, ForeignKey('games.id'))
    team_id = Column(Integer, ForeignKey('teams.id'))
    wl = Column(String)
    points = Column(Integer)
    fgm = Column(Integer)
    fga = Column(Integer)
    fg_pct = Column(Float)
    fg3m = Column(Integer)
    fg3a = Column(Integer)
    fg3_pct = Column(Float)
    ftm = Column(Integer)
    fta = Column(Integer)
    ft_pct = Column(Float)
    oreb = Column(Integer)
    dreb = Column(Integer)
    reb = Column(Integer)
    ast = Column(Integer)
    stl = Column(Integer)
    blk = Column(Integer)
    tov = Column(Integer)
    pf = Column(Integer)
    plus_minus = Column(Integer)
    team = relationship("Team", back_populates="team_stats")
    game = relationship("Game", back_populates="team_stats")
    __table_args__ = (
        UniqueConstraint('game_id', 'team_id', name='_game_team_uc'),
    )

class Game(Base):
    __tablename__ = 'games'
    id = Column(Integer, primary_key=True)
    nba_game_id = Column(Integer, unique=True, nullable=False)
    nba_season_id = Column(Integer)
    date = Column(Date)
    matchup = Column(String)
    game_status_text = Column(String)
    season = Column(String)
    season_type = Column(String)
    home_team_id = Column(Integer, ForeignKey('teams.id'))
    away_team_id = Column(Integer, ForeignKey('teams.id'))
    live_period = Column(Integer)
    home_team = relationship("Team", foreign_keys=[home_team_id])
    away_team = relationship("Team", foreign_keys=[away_team_id])
    team_stats = relationship("TeamStats", back_populates="game")
    player_stats = relationship("PlayerStats", back_populates="game")
    adv_team_stats = relationship("AdvTeamStats", back_populates="game")
    adv_player_stats = relationship("AdvPlayerStats", back_populates="game")
class PlayerStats(Base):
    __tablename__ = 'player_stats'
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'))
    game_id = Column(Integer, ForeignKey('games.id'))
    points = Column(Integer)
    assists = Column(Integer)
    rebounds = Column(Integer)
    steals = Column(Integer)
    blocks = Column(Integer)
    turnovers = Column(Integer)
    fouls = Column(Integer)
    minutes = Column(Integer)
    fg_made = Column(Integer)
    fg_attempts = Column(Integer)
    fg_percentage = Column(Float)
    fg3_made = Column(Integer)
    fg3_attempts = Column(Integer)
    fg3_percentage = Column(Float)
    ft_made = Column(Integer)
    ft_attempts = Column(Integer)
    ft_percentage = Column(Float)
    plus_minus = Column(Integer)
    player = relationship("Player", back_populates="player_stats")
    game = relationship("Game", back_populates="player_stats")

engine = get_database_engine()  
Base.metadata.create_all(engine)
