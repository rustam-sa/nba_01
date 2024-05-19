import time
import itertools
import pandas as pd
from datetime import date
from functools import wraps
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func, and_
from sqlalchemy.orm import joinedload
from sqlalchemy.dialects.postgresql import insert
from requests.exceptions import HTTPError
from datetime import datetime
from nba_api.stats.static import teams
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.endpoints import boxscoresummaryv2
from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.endpoints import boxscoreadvancedv2

import analyze
import date_utils as date_mng
from models import Team, Player, Game, TradTeamStats, AdvTeamStats, TradPlayerStats, AdvPlayerStats
from db_config import get_database_engine, get_session


class DataManager:
    def __init__(self):
        self.nba_team_id_map, self.nba_player_id_map = self.create_id_maps()
        self.db_team_id_map = {v: k for k, v in self.nba_team_id_map.items()}
        self.db_player_id_map = {v: k for k, v in self.nba_player_id_map.items()}

    @staticmethod
    def get_engine():
        return get_database_engine()
    
    @staticmethod
    def get_session():
        return get_session()

    @staticmethod
    def session_management(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            session = self.get_session()
            try:
                return method(self, session, *args, **kwargs)
            finally:
                session.close()
        return wrapper

    @staticmethod
    def pull_teams():
        nba_teams = teams.get_teams()
        return nba_teams

    def pull_players(self):
        teams = self.query_teams()
        rosters = []
        for team in teams:
            retries = 5
            for attempt in range(retries):
                try:
                    roster = commonteamroster.CommonTeamRoster(team_id=team.nba_team_id)
                    roster_df = roster.get_data_frames()[0]
                    time.sleep(0.5)
                        
                except HTTPError as e:
                    if e.response.status_code == 429:
                        wait = (attempt + 1) * 2  # Exponential back-off
                        print(f"Rate limit hit, retrying in {wait} seconds...")
                        time.sleep(wait)
                    else:
                        print(f"HTTP error occurred: {e}")
                        break
                except Exception as e:
                    print(f"An error occurred: {e}")
                    break

                roster_df["db_team_id"] = team.id
                rosters.append(roster_df)

        players = pd.concat(rosters, axis=0, ignore_index=True)
        return players
    
    @staticmethod
    def pull_games_by_team_and_season(team, season, season_type):
        try:
            gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team.nba_team_id, season_nullable=season, season_type_nullable=season_type)
            games_df = gamefinder.get_data_frames()[0]
            print(games_df)
            return games_df

        except Exception as e:
            print(f"An error occurred: {e}")
            print(gamefinder.get_json())

    @session_management
    def query_players(self, session):
        return session.query(Player).all()

    @session_management
    def query_teams(self, session):
        return session.query(Team).all()
    
    def sync_all_team_and_player_records(self):
        self.sync_all_team_records()
        self.sync_all_player_records()
    
    def sync_all_player_records(self):
        players = self.pull_players()
        for _, player_row in players.iterrows():
            player = player_row.to_dict()
            self.sync_player_record(player)

    def sync_all_team_records(self):
        teams = self.pull_teams()
        for team in teams:
            self.sync_team_record(team)

    @session_management
    def sync_player_record(self, session, player):
        if not isinstance(player, dict):
            raise TypeError(f"Expected player to be a dictionary, got {type(player)} instead.")

        insert_stmt = insert(Player).values(
            nba_player_id=player["PLAYER_ID"],
            team_id = self.db_team_id_map[player['TeamID']],
            name=player['PLAYER'],
            nickname=player["NICKNAME"],
            player_slug=player['PLAYER_SLUG'],
            jersey_number=player['NUM'],
            position=player['POSITION'],
            height=player['HEIGHT'],
            weight=player['WEIGHT'],
            birth_date=player['BIRTH_DATE'],
            age=player['AGE'],
            experience=player['EXP'],
            school=player['SCHOOL'],
            how_acquired=player['HOW_ACQUIRED']
        )

        do_update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['nba_player_id'],
            set_={
                'team_id': self.db_team_id_map[player['TeamID']],
                'name': player['PLAYER'],
                'nickname': player["NICKNAME"],
                'player_slug': player['PLAYER_SLUG'],
                'jersey_number': player['NUM'],
                'position': player['POSITION'],
                'height': player['HEIGHT'],
                'weight': player['WEIGHT'],
                'birth_date': player['BIRTH_DATE'],
                'age': player['AGE'],
                'experience': player['EXP'],
                'school': player['SCHOOL'],
                'how_acquired': player['HOW_ACQUIRED']
            }
        )

        session.execute(do_update_stmt)
        session.commit()

    @session_management
    def sync_team_record(self, session, team):
        insert_stmt = insert(Team).values(
            nba_team_id = team["id"],
            nickname = team["nickname"],
            city = team["city"],
            state = team["state"],
            full_name = team["full_name"],
            abbreviation = team["abbreviation"],
            )

        do_update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['nba_team_id'], # possible conflicting column
            set_=dict(
                nickname=insert_stmt.excluded.nickname,
                city=insert_stmt.excluded.city,
                state=insert_stmt.excluded.state,
                full_name=insert_stmt.excluded.full_name,
                abbreviation=insert_stmt.excluded.abbreviation
            )
        )

        session.execute(do_update_stmt)
        session.commit()

    def create_id_maps(self):   
        teams = self.query_teams()
        players = self.query_players()
        team_id_map = {}
        player_id_map = {}
        for team in teams:
            team_id_map[team.id] = team.nba_team_id

        for player in players:
            player_id_map[player.id] = player.nba_player_id

        return team_id_map, player_id_map

    @staticmethod
    def pull_traditional_stats_for_game(nba_game_id):
        boxscore_traditional = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=nba_game_id)
        
        player_stats = boxscore_traditional.player_stats.get_data_frame()
        team_stats = boxscore_traditional.team_stats.get_data_frame()
        player_stats = player_stats[player_stats["COMMENT"] == ""]
        return player_stats, team_stats
    
    @staticmethod
    def pull_advanced_stats_for_game(nba_game_id):
        boxscore_advanced = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=nba_game_id)

        player_stats = boxscore_advanced.player_stats.get_data_frame()
        team_stats = boxscore_advanced.team_stats.get_data_frame()
        player_stats = player_stats[player_stats["COMMENT"] == ""]
        return player_stats, team_stats
    
    def pull_all_games_from_season(self, season, season_type):
        teams = self.query_teams()
        all_games = []
        for team in teams:
            team_games = self.pull_games_by_team_and_season(team, season, season_type)
            all_games.append(team_games)
        all_games = pd.concat(all_games, axis=0, ignore_index=True)
        return all_games

    @staticmethod
    def pull_game_summary(game_id):
            game_summary = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
            game_summary_df = game_summary.game_summary.get_data_frame()
            return game_summary_df
    
    @session_management
    def sync_game(self, session, game, season, season_type):

        try:
            game_summary = self.pull_game_summary(game.loc['GAME_ID'])
            game = pd.DataFrame(game).T
            game_complete_row = pd.merge(game, game_summary, on="GAME_ID", how="inner")

            # Ensure unique index labels and access values
            nba_game_id = int(game_complete_row.loc[0, 'GAME_ID'])
            date = str(game_complete_row.loc[0, 'GAME_DATE'])
            game_status_text = str(game_complete_row.loc[0, 'GAME_STATUS_TEXT'])
            home_team_id = int(self.db_team_id_map[game_complete_row.loc[0, 'HOME_TEAM_ID']])
            away_team_id = int(self.db_team_id_map[game_complete_row.loc[0, 'VISITOR_TEAM_ID']])
            live_period = int(game_complete_row.loc[0, 'LIVE_PERIOD'])

            # Create insert statement
            insert_statement = insert(Game).values(
                nba_game_id=nba_game_id,
                date=date,
                game_status_text=game_status_text,
                season=season,
                season_type=season_type,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                live_period=live_period,
            )

            # Define fields to update on conflict
            update_fields = {
                'date': insert_statement.excluded.date,
                'game_status_text': insert_statement.excluded.game_status_text,
                'season': insert_statement.excluded.season,
                'season_type': insert_statement.excluded.season_type,
                'home_team_id': insert_statement.excluded.home_team_id,
                'away_team_id': insert_statement.excluded.away_team_id,
                'live_period': insert_statement.excluded.live_period,
            }

            # Create upsert statement
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=['nba_game_id'],
                set_=update_fields
            ).returning(Game.id)

            # Execute the upsert statement and commit
            result = session.execute(upsert_statement)
            session.commit()

            # Return the game ID
            game_id = result.fetchone()[0]
            return game_id

        except Exception as e:
            # Rollback in case of exception and raise the error
            session.rollback()
            raise RuntimeError(f"Error syncing game {game.loc['GAME_ID']}: {str(e)}")
        
    @session_management
    def sync_trad_team_stats(self, session, trad_team_stats, db_game_id):
        db_ids = []
        try:
            for _, stat_line in trad_team_stats.iterrows():
                game_id = int(db_game_id)
                team_id = int(self.db_team_id_map[stat_line['TEAM_ID']])
                if stat_line["MIN"]:
                    minutes = stat_line["MIN"].split(':')[0] 
                    minutes = float(minutes)
                else:
                    minutes = 0
                print(stat_line)
                fgm = 0 if stat_line['FGM'] is None else int(stat_line["FGM"])
                fga = int(stat_line["FGA"])
                fg_pct = float(stat_line["FG_PCT"])
                fg3m = int(stat_line["FG3M"])
                fg3a = int(stat_line["FG3A"])
                fg3_pct = float(stat_line["FG3_PCT"])
                ftm = int(stat_line["FTM"])
                fta = int(stat_line["FTA"])
                ft_pct = float(stat_line["FT_PCT"])
                oreb = int(stat_line["OREB"])
                dreb = int(stat_line["DREB"])
                reb = int(stat_line["REB"])
                ast = int(stat_line["AST"])
                stl = int(stat_line["STL"])
                blk = int(stat_line["BLK"])
                to = int(stat_line["TO"])
                pf = int(stat_line["PF"])
                pts = int(stat_line["PTS"])
                plus_minus = int(stat_line["PLUS_MINUS"])

                # Create insert statement
                insert_statement = insert(TradTeamStats).values(
                    game_id=game_id,
                    team_id=team_id,
                    minutes=minutes,
                    fgm=fgm,
                    fga=fga,
                    fg_pct=fg_pct,
                    fg3m=fg3m,
                    fg3a=fg3a,
                    fg3_pct=fg3_pct,
                    ftm=ftm,
                    fta=fta,
                    ft_pct=ft_pct,
                    oreb=oreb,
                    dreb=dreb,
                    reb=reb,
                    ast=ast,
                    stl=stl,
                    blk=blk,
                    to=to,
                    pf=pf,
                    pts=pts,
                    plus_minus=plus_minus
                )

                # Define fields to update on conflict
                update_fields = {
                    'minutes': insert_statement.excluded.minutes,
                    'fgm': insert_statement.excluded.fgm,
                    'fga': insert_statement.excluded.fga,
                    'fg_pct': insert_statement.excluded.fg_pct,
                    'fg3m': insert_statement.excluded.fg3m,
                    'fg3a': insert_statement.excluded.fg3a,
                    'fg3_pct': insert_statement.excluded.fg3_pct,
                    'ftm': insert_statement.excluded.ftm,
                    'fta': insert_statement.excluded.fta,
                    'ft_pct': insert_statement.excluded.ft_pct,
                    'oreb': insert_statement.excluded.oreb,
                    'dreb': insert_statement.excluded.dreb,
                    'reb': insert_statement.excluded.reb,
                    'ast': insert_statement.excluded.ast,
                    'stl': insert_statement.excluded.stl,
                    'blk': insert_statement.excluded.blk,
                    'to': insert_statement.excluded.to,
                    'pf': insert_statement.excluded.pf,
                    'pts': insert_statement.excluded.pts,
                    'plus_minus': insert_statement.excluded.plus_minus
                }

                # Create upsert statement
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=['game_id', 'team_id'],
                    set_=update_fields
                ).returning(TradTeamStats.id)

                # Execute the upsert statement and commit
                result = session.execute(upsert_statement)
                session.commit()
                db_id = result.fetchone()[0]  # Extract the ID
                db_ids.append(db_id)
        except Exception as e:
            # Rollback in case of exception and raise the error
            session.rollback()
            raise RuntimeError(f"Error syncing game {game_id}: {str(e)}")
        
        return db_ids

    @session_management
    def sync_adv_team_stats(self, session, adv_team_stats, db_game_id):
        db_ids = []
        try:
            for _, stat_line in adv_team_stats.iterrows():
                game_id = int(db_game_id)
                team_id = int(self.db_team_id_map[stat_line['TEAM_ID']])
                minutes = stat_line["MIN"].split(':')[0] 
                minutes = float(minutes)
                e_off_rating = float(stat_line["E_OFF_RATING"])
                off_rating = float(stat_line["OFF_RATING"])
                e_def_rating = float(stat_line["E_DEF_RATING"])
                def_rating = float(stat_line["DEF_RATING"])
                e_net_rating = float(stat_line["E_NET_RATING"])
                net_rating = float(stat_line["NET_RATING"])
                ast_pct = float(stat_line["AST_PCT"])
                ast_tov = float(stat_line["AST_TOV"])
                ast_ratio = float(stat_line["AST_RATIO"])
                oreb_pct = float(stat_line["OREB_PCT"])
                dreb_pct = float(stat_line["DREB_PCT"])
                reb_pct = float(stat_line["REB_PCT"])
                e_tm_tov_pct = float(stat_line["E_TM_TOV_PCT"])
                tm_tov_pct = float(stat_line["TM_TOV_PCT"])
                efg_pct = float(stat_line["EFG_PCT"])
                ts_pct = float(stat_line["TS_PCT"])
                usg_pct = float(stat_line["USG_PCT"])
                e_usg_pct = float(stat_line["E_USG_PCT"])
                e_pace = float(stat_line["E_PACE"])
                pace = float(stat_line["PACE"])
                pace_per40 = float(stat_line["PACE_PER40"])
                poss = int(stat_line["POSS"])
                pie = float(stat_line["PIE"])

                # Create insert statement
                insert_statement = insert(AdvTeamStats).values(
                    game_id=game_id,
                    team_id=team_id,
                    minutes=minutes,
                    e_off_rating=e_off_rating,
                    off_rating=off_rating,
                    e_def_rating=e_def_rating,
                    def_rating=def_rating,
                    e_net_rating=e_net_rating,
                    net_rating=net_rating,
                    ast_pct=ast_pct,
                    ast_tov=ast_tov,
                    ast_ratio=ast_ratio,
                    oreb_pct=oreb_pct,
                    dreb_pct=dreb_pct,
                    reb_pct=reb_pct,
                    e_tm_tov_pct=e_tm_tov_pct,
                    tm_tov_pct=tm_tov_pct,
                    efg_pct=efg_pct,
                    ts_pct=ts_pct,
                    usg_pct=usg_pct,
                    e_usg_pct=e_usg_pct,
                    e_pace=e_pace,
                    pace=pace,
                    pace_per40=pace_per40,
                    poss=poss,
                    pie=pie
                )

                # Define fields to update on conflict
                update_fields = {
                    'minutes': insert_statement.excluded.minutes,
                    'e_off_rating': insert_statement.excluded.e_off_rating,
                    'off_rating': insert_statement.excluded.off_rating,
                    'e_def_rating': insert_statement.excluded.e_def_rating,
                    'def_rating': insert_statement.excluded.def_rating,
                    'e_net_rating': insert_statement.excluded.e_net_rating,
                    'net_rating': insert_statement.excluded.net_rating,
                    'ast_pct': insert_statement.excluded.ast_pct,
                    'ast_tov': insert_statement.excluded.ast_tov,
                    'ast_ratio': insert_statement.excluded.ast_ratio,
                    'oreb_pct': insert_statement.excluded.oreb_pct,
                    'dreb_pct': insert_statement.excluded.dreb_pct,
                    'reb_pct': insert_statement.excluded.reb_pct,
                    'e_tm_tov_pct': insert_statement.excluded.e_tm_tov_pct,
                    'tm_tov_pct': insert_statement.excluded.tm_tov_pct,
                    'efg_pct': insert_statement.excluded.efg_pct,
                    'ts_pct': insert_statement.excluded.ts_pct,
                    'usg_pct': insert_statement.excluded.usg_pct,
                    'e_usg_pct': insert_statement.excluded.e_usg_pct,
                    'e_pace': insert_statement.excluded.e_pace,
                    'pace': insert_statement.excluded.pace,
                    'pace_per40': insert_statement.excluded.pace_per40,
                    'poss': insert_statement.excluded.poss,
                    'pie': insert_statement.excluded.pie
                }

                # Create upsert statement
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=['game_id', 'team_id'],
                    set_=update_fields
                ).returning(AdvTeamStats.id)

                # Execute the upsert statement and commit
                result = session.execute(upsert_statement)
                session.commit()
                db_id = result.fetchone()[0]  # Extract the ID
                db_ids.append(db_id)
        except Exception as e:
            # Rollback in case of exception and raise the error
            session.rollback()
            raise RuntimeError(f"Error syncing game {game_id}: {str(e)}")
        
        return db_ids
    
    @session_management
    def sync_trad_player_stats(self, session, trad_player_stats, db_game_id):
        db_ids = []
        try:
            for _, stat_line in trad_player_stats.iterrows():
                game_id = int(db_game_id)
                if stat_line['PLAYER_ID'] not in self.db_player_id_map:
                    continue
                player_id = int(self.db_player_id_map[stat_line['PLAYER_ID']])
                start_position = str(stat_line["START_POSITION"])
                minutes = stat_line["MIN"].split(':')[0] 
                minutes = float(minutes)
                fgm = int(stat_line["FGM"])
                fga = int(stat_line["FGA"])
                fg_pct = float(stat_line["FG_PCT"])
                fg3m = int(stat_line["FG3M"])
                fg3a = int(stat_line["FG3A"])
                fg3_pct = float(stat_line["FG3_PCT"])
                ftm = int(stat_line["FTM"])
                fta = int(stat_line["FTA"])
                ft_pct = float(stat_line["FT_PCT"])
                oreb = int(stat_line["OREB"])
                dreb = int(stat_line["DREB"])
                reb = int(stat_line["REB"])
                ast = int(stat_line["AST"])
                stl = int(stat_line["STL"])
                blk = int(stat_line["BLK"])
                to = int(stat_line["TO"])
                pf = int(stat_line["PF"])
                pts = int(stat_line["PTS"])
                plus_minus = int(stat_line["PLUS_MINUS"])

                # Create insert statement
                insert_statement = insert(TradPlayerStats).values(
                    game_id=game_id,
                    player_id=player_id,
                    start_position=start_position,
                    minutes=minutes,
                    fgm=fgm,
                    fga=fga,
                    fg_pct=fg_pct,
                    fg3m=fg3m,
                    fg3a=fg3a,
                    fg3_pct=fg3_pct,
                    ftm=ftm,
                    fta=fta,
                    ft_pct=ft_pct,
                    oreb=oreb,
                    dreb=dreb,
                    reb=reb,
                    ast=ast,
                    stl=stl,
                    blk=blk,
                    to=to,
                    pf=pf,
                    pts=pts,
                    plus_minus=plus_minus
                )

                # Define fields to update on conflict
                update_fields = {
                    'start_position': insert_statement.excluded.start_position,
                    'minutes': insert_statement.excluded.minutes,
                    'fgm': insert_statement.excluded.fgm,
                    'fga': insert_statement.excluded.fga,
                    'fg_pct': insert_statement.excluded.fg_pct,
                    'fg3m': insert_statement.excluded.fg3m,
                    'fg3a': insert_statement.excluded.fg3a,
                    'fg3_pct': insert_statement.excluded.fg3_pct,
                    'ftm': insert_statement.excluded.ftm,
                    'fta': insert_statement.excluded.fta,
                    'ft_pct': insert_statement.excluded.ft_pct,
                    'oreb': insert_statement.excluded.oreb,
                    'dreb': insert_statement.excluded.dreb,
                    'reb': insert_statement.excluded.reb,
                    'ast': insert_statement.excluded.ast,
                    'stl': insert_statement.excluded.stl,
                    'blk': insert_statement.excluded.blk,
                    'to': insert_statement.excluded.to,
                    'pf': insert_statement.excluded.pf,
                    'pts': insert_statement.excluded.pts,
                    'plus_minus': insert_statement.excluded.plus_minus
                }

                # Create upsert statement
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=['game_id', 'player_id'],
                    set_=update_fields
                ).returning(TradPlayerStats.id)

                # Execute the upsert statement and commit
                result = session.execute(upsert_statement)
                session.commit()
                db_id = result.fetchone()[0]  # Extract the ID
                db_ids.append(db_id)
        except Exception as e:
            # Rollback in case of exception and raise the error
            session.rollback()
            raise RuntimeError(f"Error syncing game {game_id}: {str(e)}")
        
        return db_ids
    
    @session_management
    def sync_adv_player_stats(self, session, adv_player_stats, db_game_id):
        db_ids = []
        try:
            for _, stat_line in adv_player_stats.iterrows():
                game_id = int(db_game_id)
                if stat_line['PLAYER_ID'] not in self.db_player_id_map:
                    continue
                player_id = int(self.db_player_id_map[stat_line['PLAYER_ID']])
                minutes = float(stat_line["MIN"].split(':')[0])  # Extract the minutes part and convert to float
                e_off_rating = float(stat_line["E_OFF_RATING"])
                off_rating = float(stat_line["OFF_RATING"])
                e_def_rating = float(stat_line["E_DEF_RATING"])
                def_rating = float(stat_line["DEF_RATING"])
                e_net_rating = float(stat_line["E_NET_RATING"])
                net_rating = float(stat_line["NET_RATING"])
                ast_pct = float(stat_line["AST_PCT"])
                ast_tov = float(stat_line["AST_TOV"])
                ast_ratio = float(stat_line["AST_RATIO"])
                oreb_pct = float(stat_line["OREB_PCT"])
                dreb_pct = float(stat_line["DREB_PCT"])
                reb_pct = float(stat_line["REB_PCT"])
                tm_tov_pct = float(stat_line["TM_TOV_PCT"])
                efg_pct = float(stat_line["EFG_PCT"])
                ts_pct = float(stat_line["TS_PCT"])
                usg_pct = float(stat_line["USG_PCT"])
                e_usg_pct = float(stat_line["E_USG_PCT"])
                e_pace = float(stat_line["E_PACE"])
                pace = float(stat_line["PACE"])
                pace_per40 = float(stat_line["PACE_PER40"])
                poss = int(stat_line["POSS"])
                pie = float(stat_line["PIE"])

                # Create insert statement
                insert_statement = insert(AdvPlayerStats).values(
                    game_id=game_id,
                    player_id=player_id,
                    minutes=minutes,
                    e_off_rating=e_off_rating,
                    off_rating=off_rating,
                    e_def_rating=e_def_rating,
                    def_rating=def_rating,
                    e_net_rating=e_net_rating,
                    net_rating=net_rating,
                    ast_pct=ast_pct,
                    ast_tov=ast_tov,
                    ast_ratio=ast_ratio,
                    oreb_pct=oreb_pct,
                    dreb_pct=dreb_pct,
                    reb_pct=reb_pct,
                    tm_tov_pct=tm_tov_pct,
                    efg_pct=efg_pct,
                    ts_pct=ts_pct,
                    usg_pct=usg_pct,
                    e_usg_pct=e_usg_pct,
                    e_pace=e_pace,
                    pace=pace,
                    pace_per40=pace_per40,
                    poss=poss,
                    pie=pie
                )

                # Define fields to update on conflict
                update_fields = {
                    'minutes': insert_statement.excluded.minutes,
                    'e_off_rating': insert_statement.excluded.e_off_rating,
                    'off_rating': insert_statement.excluded.off_rating,
                    'e_def_rating': insert_statement.excluded.e_def_rating,
                    'def_rating': insert_statement.excluded.def_rating,
                    'e_net_rating': insert_statement.excluded.e_net_rating,
                    'net_rating': insert_statement.excluded.net_rating,
                    'ast_pct': insert_statement.excluded.ast_pct,
                    'ast_tov': insert_statement.excluded.ast_tov,
                    'ast_ratio': insert_statement.excluded.ast_ratio,
                    'oreb_pct': insert_statement.excluded.oreb_pct,
                    'dreb_pct': insert_statement.excluded.dreb_pct,
                    'reb_pct': insert_statement.excluded.reb_pct,
                    'tm_tov_pct': insert_statement.excluded.tm_tov_pct,
                    'efg_pct': insert_statement.excluded.efg_pct,
                    'ts_pct': insert_statement.excluded.ts_pct,
                    'usg_pct': insert_statement.excluded.usg_pct,
                    'e_usg_pct': insert_statement.excluded.e_usg_pct,
                    'e_pace': insert_statement.excluded.e_pace,
                    'pace': insert_statement.excluded.pace,
                    'pace_per40': insert_statement.excluded.pace_per40,
                    'poss': insert_statement.excluded.poss,
                    'pie': insert_statement.excluded.pie
                }

                # Create upsert statement
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=['game_id', 'player_id'],
                    set_=update_fields
                ).returning(AdvPlayerStats.id)

                # Execute the upsert statement and commit
                result = session.execute(upsert_statement)
                session.commit()
                db_id = result.fetchone()[0]  # Extract the ID
                db_ids.append(db_id)
        except Exception as e:
            # Rollback in case of exception and raise the error
            session.rollback()
            raise RuntimeError(f"Error syncing game {game_id}: {str(e)}")
        
        return db_ids
    
    def sync_games(self, season, season_type):
        games = self.pull_all_games_from_season(season, season_type)
        for _, game in games.iterrows():
            time.sleep(0.3)
            nba_game_id = game['GAME_ID']
            adv_player_stats, adv_team_stats = self.pull_advanced_stats_for_game(nba_game_id)
            trad_player_stats, trad_team_stats = self.pull_traditional_stats_for_game(nba_game_id)
            db_game_id = self.sync_game(game, season, season_type)
            self.sync_trad_team_stats(trad_team_stats=trad_team_stats, db_game_id=db_game_id)
            self.sync_adv_team_stats(adv_team_stats=adv_team_stats, db_game_id=db_game_id)
            self.sync_trad_player_stats(trad_player_stats=trad_player_stats, db_game_id=db_game_id)
            self.sync_adv_player_stats(adv_player_stats=adv_player_stats, db_game_id=db_game_id)
        
    @session_management
    def query_games(self, session):
        return session.query(Game).all()
    
    @session_management
    def query_advanced_player_stats(self, session):
        return session.query(AdvPlayerStats).all()
    
    @staticmethod
    def convert_to_df(query_result, exclude_columns=['_sa_instance_state']):
        if not query_result:
            return pd.DataFrame()
        data = [{key: value for key, value in vars(obj).items() if key not in exclude_columns} for obj in query_result]
        return pd.DataFrame(data)
    
    @session_management
    def get_player_id(self, session, player_name):
        player = session.query(Player).filter(Player.name==player_name).all()[0]
        player_id = player.id
        return player_id
    
    @session_management
    def get_and_save_player_data(self, session, player_id, filename=None):
        data = session.query(
            Player,
            TradPlayerStats,
            AdvPlayerStats,
            Game
        ).join(Game, TradPlayerStats.game_id == Game.id)\
        .join(Player, TradPlayerStats.player_id == Player.id)\
        .join(AdvPlayerStats, and_(TradPlayerStats.game_id == AdvPlayerStats.game_id, 
                                    TradPlayerStats.player_id == AdvPlayerStats.player_id))\
        .filter(
            (TradPlayerStats.player_id == player_id) #&
            #(Game.season_type == season_type)
        ).all()

        # Convert the query result to a DataFrame
        data_list = []
        for player, trad_stats, adv_stats, game in data:
            row = {
                'player_name': player.name,
                'player_position': player.position,
                'minutes': trad_stats.minutes,
                'points': trad_stats.pts,
                'rebounds': trad_stats.reb,
                'assists': trad_stats.ast,
                'efg': adv_stats.efg_pct,
                'fg3a': trad_stats.fg3a,
                'fg3m': trad_stats.fg3m,
                'fg3_pct': trad_stats.fg3_pct,
                'fga': trad_stats.fga,
                'fgm': trad_stats.fgm,
                'fta': trad_stats.fta,
                'ft_pct': trad_stats.ft_pct, 
                'steals': trad_stats.stl,
                'blocks': trad_stats.blk,
                'date': game.date,

            }
            data_list.append(row)

        data_df = pd.DataFrame(data_list)
        data_df = data_df.sort_values(by="date", ascending=False)
        save_destination = player.name if not filename else filename
        data_df.to_csv(f"data_pile/{save_destination}.csv")
        return data_df
    
    @staticmethod
    def extract_raw_data(file_path): # .csv
    # gets input from A1
    # Sample input text (use the content of your file here)
        raw_input = pd.read_csv(file_path)
        list_of_raw_input = list(raw_input.iloc[:, 0])
        return list_of_raw_input
    
    def load_available_props(self): # hardrock bet 
        raw_input = self.extract_raw_data("prop_lines/prop_lines.csv")
        print("Received raw input.")
        stat_names = {
                 'PointsSGP': "points",
                'AssistsSGP': "assists",
            'Threes MadeSGP': "fg3m",
               'ReboundsSGP': "rebounds",
       'Field Goals MadeSGP': "fgm",
                 'StealsSGP': "steals",
                 'BlocksSGP': "blocks",
            }
        #debug stat_name_inputs = extract_raw_data("prop_lines/player_prop_categories.csv")
        players = self.query_players()
        player_names = [player.name for player in players]
        teams = self.query_teams()
        team_names = [team.nickname for team in teams]
        row_of_interest = 0
        current_category = None
        current_player = None
        current_team = None
        records = []
        for _, item in enumerate(raw_input):
            if item is None: 
                continue
            if item in stat_names:
                current_category = stat_names[item]
                print(f"Loading {current_category} props.")
            if item != current_team:
                if item in team_names:
                    current_team = item
            if item in player_names:
                current_player = item 
                assert current_team
                record = [current_player, current_team, current_category]
                row_of_interest = 6


            if row_of_interest:
                row_of_interest -= 1
                if row_of_interest < 5:
                    record.append(item)
                    if row_of_interest == 1:
                        records.append(record)
                        record = []
        df = pd.DataFrame.from_records(records, columns=["player_name", "team", "stat", "over_threshold", "over_odds", "under_threshold", "under_odds"])
        
        df['player_name'] = df['player_name'].astype(str)
        df['team'] = df['team'].astype(str)
        df['stat'] = df['stat'].astype(str)
        df['over_threshold'] = df['over_threshold'].str.extract(r'(\d+\.\d+)').astype(float)
        df['under_threshold'] = df['under_threshold'].str.extract(r'(\d+\.\d+)').astype(float)
        df['over_odds'] = df['over_odds'].astype(int)
        df['under_odds'] = df['under_odds'].astype(int)
        print(df)
        return df
    
    def get_analyzed_props(self):

        available_props = self.load_available_props()
        props = []
        for _, row in available_props.iterrows():
            print(row)
            for bet_type in ["over", "under"]:
                prop = Prop(
                        name=row["player_name"], 
                        team=row["team"],
                        stat=row["stat"], 
                    threshold=row[f"{bet_type}_threshold"], 
                        odds=row[f"{bet_type}_odds"], 
                    bet_type=bet_type
                    )
                props.append(prop)    
                print("Prop object created.")

        return props
    
    def generate_heterogenous_combinations(df, n):

        # Generate all combinations of n rows
        combinations = list(itertools.combinations(df.index, n))

        # Function to evaluate heterogeneity of a combination
        def evaluate_heterogeneity(comb, df):
            comb_list = list(comb)
            players = df.loc[comb_list, 'PLAYER']
            stats = df.loc[comb_list, 'STAT']
            teams = df.loc[comb_list, 'TEAM']
            # Calculate a simple heterogeneity score (you can define your own logic)
            player_score = len(set(players))
            stat_score = len(set(stats))
            team_score = len(set(teams))
            return player_score + stat_score + team_score

        # Evaluate all combinations and sort them by heterogeneity score
        comb_scores = [(comb, evaluate_heterogeneity(comb, df)) for comb in combinations]
        comb_scores_sorted = sorted(comb_scores, key=lambda x: x[1], reverse=True)

        # Select the most heterogeneous combinations (you can define how many you want)
        top_combinations = comb_scores_sorted # Top 5 combinations for example

        # Display the most heterogeneous combinations
        for comb, score in top_combinations:
            print(f"Combination: {comb}, Score: {score}")
            print(df.loc[list(comb)])
            print()

        # Optional: Convert combinations to DataFrame
        top_comb_dfs = [(df.loc[list(comb)], score) for comb, score in top_combinations]

        return top_comb_dfs
    
    
    @staticmethod
    def filter_props(analyzed_props, filter_dict):
        print(len(analyzed_props))
        analyzed_props = [prop for prop in analyzed_props if prop.ev > 0]
        print(f"num of profitable props: {len(analyzed_props)}")
        analyzed_props = pd.DataFrame.from_dict([prop.entry for prop in analyzed_props])
        for key in filter_dict.keys():
            for filter_item, category in filter_dict[key]:
                print(f'Filtering {filter_item}')
                analyzed_props = analyzed_props[analyzed_props[category] != filter_item]
        print(len(analyzed_props))
        print(analyzed_props.head())
        filtered_df = analyzed_props.sort_values(by="PROB", ascending=False).head(36)
        filtered_df.to_csv(f"props_{date.today()}.csv")
        return filtered_df
    
    @session_management
    def get_and_save_team_data(self, session, team_id, filename=None):
        data = session.query(
            Team,
            TradTeamStats,
            AdvTeamStats,
            Game
        ).join(Game, TradTeamStats.game_id == Game.id)\
        .join(Team, TradTeamStats.team_id == Team.id)\
        .join(AdvTeamStats, and_(TradTeamStats.game_id == AdvTeamStats.game_id, 
                                    TradTeamStats.team_id == AdvTeamStats.team_id))\
        .filter(
            (TradTeamStats.team_id == team_id) #&
            #(Game.season_type == season_type)
        ).all()

        # Convert the query result to a DataFrame
        data_list = []
        for team, trad_stats, adv_stats, game in data:
            row = {
                'team_name': team.full_name,
                'points': trad_stats.pts,
                'rebounds': trad_stats.reb,
                'assists': trad_stats.ast,
                'efg': adv_stats.efg_pct,
                'fg3a': trad_stats.fg3a,
                'fg3m': trad_stats.fg3m,
                'fg3_pct': trad_stats.fg3_pct,
                'fga': trad_stats.fga,
                'fgm': trad_stats.fgm,
                'fta': trad_stats.fta,
                'ft_pct': trad_stats.ft_pct, 
                'steals': trad_stats.stl,
                'blocks': trad_stats.blk,
                'to': trad_stats.to, 
                'date': game.date,

            }
            data_list.append(row)

        data_df = pd.DataFrame(data_list)
        data_df = data_df.sort_values(by="date", ascending=False)
        save_destination = team.nickname if not filename else filename
        data_df.to_csv(f"data_pile/{save_destination}.csv")
        return data_df
    
    @staticmethod
    def filter_props(analyzed_props, filter_players=None, n_props = 36):
        print(len(analyzed_props))
        analyzed_props = [prop for prop in analyzed_props if prop.ev > 0]
        print(f"num of profitable props: {len(analyzed_props)}")
        analyzed_props = pd.DataFrame.from_dict([prop.entry for prop in analyzed_props])
        print(analyzed_props.head(5))
        if filter_players:
            for player in filter_players:
                analyzed_props = analyzed_props[analyzed_props['PLAYER'] != player]
        print(len(analyzed_props))
        print(analyzed_props.head())
        filtered_df = analyzed_props.sort_values(by="PROB", ascending=False).head(n_props)
        filtered_df.to_csv(f"props_{date.today()}.csv")
        return filtered_df


class Prop:
    def __init__(self, name, team, stat, threshold, odds, bet_type):
        self.name = name
        self.team = team
        self.stat = stat
        self.n = threshold
        self.odds = odds
        self.bet_type = bet_type
        self.probability = self.get_prop_probability()
        self.ev, self.house_prob = self.get_ev_and_implied_prob()
        self.print_out = f"""
            PLAYER: {self.name}
            STAT: {self.stat}
            THRESH: {self.n}
            ODDS: {self.odds}
            TYPE: {self.bet_type}
            PROB: {self.probability}
                EV: {self.ev}
        HOUSE_PROB: {self.house_prob}
            """
        print(self.print_out)
        self.entry = {
            "PLAYER": self.name,
            "TEAM": self.team,
            "STAT": self.stat,
            "THRESH": self.n,
            "ODDS": self.odds,
            "TYPE": self.bet_type,
            "PROB": self.probability,
                "EV": self.ev,
        "HOUSE_PROB": self.house_prob
        }
    
    def get_prop_probability(self, last_n_games=25):
        dm = DataManager()
        player_id = dm.get_player_id(self.name)
        data = dm.get_and_save_player_data(player_id, self.name).sort_values(by='date', ascending=False).head(last_n_games).copy()
        # print(data.head())
        if self.bet_type == "over":
            return analyze.estimate_probability_poisson_over(data, self.stat, self.n)
        elif self.bet_type == "under":
            return analyze.estimate_probability_poisson_under(data, self.stat, self.n)
        else:
            raise ValueError("Invalid bet type. Use 'over' or 'under'.")
        
    def get_ev_and_implied_prob(self):
        odds = self.american_to_decimal(self.odds)
        house_probability = analyze.estimate_implied_probability(odds)
        ev = analyze.calculate_ev(self.probability, odds, 5)
        return ev, house_probability
    
    @staticmethod
    def american_to_decimal(american_odds):
        """Convert American odds to decimal odds."""
        if american_odds > 0:
            return 1 + (american_odds / 100)
        else:
            return 1 + (100 / abs(american_odds))

    