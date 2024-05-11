from sqlalchemy import Column, Integer, String, ForeignKey, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from db_config import get_database_engine

Base = declarative_base()
metadata = Base.metadata


class Team(Base):
        __tablename__ = 'teams'
        id = Column(Integer, primary_key=True)
        nickname = Column(String)  
        city = Column(String)
        state = Column(String)
        full_name = Column(String)  
        abbreviation = Column(String)  
        players = relationship("Player", back_populates="team")

class Player(Base):
    __tablename__ = 'players'
    id = Column(Integer, primary_key=True)
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
    player_id = Column(Integer, unique=True)
    how_acquired = Column(String)
    team_id = Column(Integer, ForeignKey('teams.id'))
    team = relationship("Team", back_populates="players")
    stats = relationship("GameStats", back_populates="player")

class TeamStats(Base):
    __tablename__ = 'team_stats'
    id = Column(Integer, primary_key=True)
    game_id = Column(Integer, ForeignKey('games.id'))
    team_id = Column(Integer, ForeignKey('teams.id'))
    points = Column(Integer)
    field_goals_made = Column(Integer)
    field_goals_attempted = Column(Integer)
    field_goal_percentage = Column(Float)
    three_point_field_goals_made = Column(Integer)
    three_point_field_goals_attempted = Column(Integer)
    three_point_field_goal_percentage = Column(Float)
    free_throws_made = Column(Integer)
    free_throws_attempted = Column(Integer)
    free_throw_percentage = Column(Float)
    offensive_rebounds = Column(Integer)
    defensive_rebounds = Column(Integer)
    total_rebounds = Column(Integer)
    assists = Column(Integer)
    steals = Column(Integer)
    blocks = Column(Integer)
    turnovers = Column(Integer)
    personal_fouls = Column(Integer)
    plus_minus = Column(Integer)
    team = relationship("Team")
    game = relationship("Game", back_populates="team_stats")

class Game(Base):
    __tablename__ = 'games'
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    season = Column(String)
    season_type = Column(String)
    home_team_id = Column(Integer, ForeignKey('teams.id'))
    away_team_id = Column(Integer, ForeignKey('teams.id'))
    home_team = relationship("Team", foreign_keys=[home_team_id])
    away_team = relationship("Team", foreign_keys=[away_team_id])
    team_stats = relationship("TeamStats", back_populates="game")

class GameStats(Base):
    __tablename__ = 'game_stats'
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
    player = relationship("Player", back_populates="stats")

engine = get_database_engine()  
Base.metadata.create_all(engine)
