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
    trad_team_stats = relationship("TradTeamStats", back_populates="team")
    adv_team_stats = relationship("AdvTeamStats", back_populates="team")

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
    trad_player_stats = relationship("TradPlayerStats", back_populates="player")
    adv_player_stats = relationship("AdvPlayerStats", back_populates="player")

class Game(Base):
    __tablename__ = 'games'
    
    id = Column(Integer, primary_key=True)
    nba_game_id = Column(Integer, unique=True, nullable=False)
    date = Column(Date)
    game_status_text = Column(String)
    season = Column(String)
    season_type = Column(String)
    home_team_id = Column(Integer, ForeignKey('teams.id'))
    away_team_id = Column(Integer, ForeignKey('teams.id'))
    live_period = Column(Integer)
    
    home_team = relationship("Team", foreign_keys=[home_team_id])
    away_team = relationship("Team", foreign_keys=[away_team_id])
    trad_team_stats = relationship("TradTeamStats", back_populates="game")
    trad_player_stats = relationship("TradPlayerStats", back_populates="game")
    adv_team_stats = relationship("AdvTeamStats", back_populates="game")
    adv_player_stats = relationship("AdvPlayerStats", back_populates="game")

class TradTeamStats(Base):
    __tablename__ = 'trad_team_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(Integer, ForeignKey('games.id'), nullable=False)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    minutes = Column(Float, nullable=False)
    fgm = Column(Integer, nullable=False)
    fga = Column(Integer, nullable=False)
    fg_pct = Column(Float, nullable=False)
    fg3m = Column(Integer, nullable=False)
    fg3a = Column(Integer, nullable=False)
    fg3_pct = Column(Float, nullable=False)
    ftm = Column(Integer, nullable=False)
    fta = Column(Integer, nullable=False)
    ft_pct = Column(Float, nullable=False)
    oreb = Column(Integer, nullable=False)
    dreb = Column(Integer, nullable=False)
    reb = Column(Integer, nullable=False)
    ast = Column(Integer, nullable=False)
    stl = Column(Integer, nullable=False)
    blk = Column(Integer, nullable=False)
    to = Column(Integer, nullable=False)
    pf = Column(Integer, nullable=False)
    pts = Column(Integer, nullable=False)
    plus_minus = Column(Integer, nullable=False)

    __table_args__ = (UniqueConstraint('game_id', 'team_id', name='_game_team_uc'),)

    # Relationships
    game = relationship('Game', back_populates='trad_team_stats')
    team = relationship('Team', back_populates='trad_team_stats')

class TradPlayerStats(Base):
    __tablename__ = 'trad_player_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(Integer, ForeignKey('games.id'), nullable=False)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    start_position = Column(String, nullable=True)
    minutes = Column(Float, nullable=False)
    fgm = Column(Integer, nullable=False)
    fga = Column(Integer, nullable=False)
    fg_pct = Column(Float, nullable=False)
    fg3m = Column(Integer, nullable=False)
    fg3a = Column(Integer, nullable=False)
    fg3_pct = Column(Float, nullable=False)
    ftm = Column(Integer, nullable=False)
    fta = Column(Integer, nullable=False)
    ft_pct = Column(Float, nullable=False)
    oreb = Column(Integer, nullable=False)
    dreb = Column(Integer, nullable=False)
    reb = Column(Integer, nullable=False)
    ast = Column(Integer, nullable=False)
    stl = Column(Integer, nullable=False)
    blk = Column(Integer, nullable=False)
    to = Column(Integer, nullable=False)
    pf = Column(Integer, nullable=False)
    pts = Column(Integer, nullable=False)
    plus_minus = Column(Integer, nullable=False)

    __table_args__ = (UniqueConstraint('game_id', 'player_id', name='_game_player_uc'),)

    # Relationships
    game = relationship('Game', back_populates='trad_player_stats')
    player = relationship('Player', back_populates='trad_player_stats')

class AdvTeamStats(Base):
    __tablename__ = 'adv_team_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(Integer, ForeignKey('games.id'), nullable=False)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    minutes = Column(Float, nullable=False)
    e_off_rating = Column(Float, nullable=False)
    off_rating = Column(Float, nullable=False)
    e_def_rating = Column(Float, nullable=False)
    def_rating = Column(Float, nullable=False)
    e_net_rating = Column(Float, nullable=False)
    net_rating = Column(Float, nullable=False)
    ast_pct = Column(Float, nullable=False)
    ast_tov = Column(Float, nullable=False)
    ast_ratio = Column(Float, nullable=False)
    oreb_pct = Column(Float, nullable=False)
    dreb_pct = Column(Float, nullable=False)
    reb_pct = Column(Float, nullable=False)
    e_tm_tov_pct = Column(Float, nullable=False)
    tm_tov_pct = Column(Float, nullable=False)
    efg_pct = Column(Float, nullable=False)
    ts_pct = Column(Float, nullable=False)
    usg_pct = Column(Float, nullable=False)
    e_usg_pct = Column(Float, nullable=False)
    e_pace = Column(Float, nullable=False)
    pace = Column(Float, nullable=False)
    pace_per40 = Column(Float, nullable=False)
    poss = Column(Integer, nullable=False)
    pie = Column(Float, nullable=False)

    __table_args__ = (UniqueConstraint('game_id', 'team_id', name='_adv_game_team_uc'),)

    # Relationships
    game = relationship('Game', back_populates='adv_team_stats')
    team = relationship('Team', back_populates='adv_team_stats')

class AdvPlayerStats(Base):
    __tablename__ = 'adv_player_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(Integer, ForeignKey('games.id'), nullable=False)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    minutes = Column(Float, nullable=False)
    e_off_rating = Column(Float, nullable=False)
    off_rating = Column(Float, nullable=False)
    e_def_rating = Column(Float, nullable=False)
    def_rating = Column(Float, nullable=False)
    e_net_rating = Column(Float, nullable=False)
    net_rating = Column(Float, nullable=False)
    ast_pct = Column(Float, nullable=False)
    ast_tov = Column(Float, nullable=False)
    ast_ratio = Column(Float, nullable=False)
    oreb_pct = Column(Float, nullable=False)
    dreb_pct = Column(Float, nullable=False)
    reb_pct = Column(Float, nullable=False)
    tm_tov_pct = Column(Float, nullable=False)
    efg_pct = Column(Float, nullable=False)
    ts_pct = Column(Float, nullable=False)
    usg_pct = Column(Float, nullable=False)
    e_usg_pct = Column(Float, nullable=False)
    e_pace = Column(Float, nullable=False)
    pace = Column(Float, nullable=False)
    pace_per40 = Column(Float, nullable=False)
    poss = Column(Integer, nullable=False)
    pie = Column(Float, nullable=False)

    __table_args__ = (UniqueConstraint('game_id', 'player_id', name='_adv_game_player_uc'),)

    # Relationships
    game = relationship('Game', back_populates='adv_player_stats')
    player = relationship('Player', back_populates='adv_player_stats')

engine = get_database_engine()  
Base.metadata.create_all(engine)

