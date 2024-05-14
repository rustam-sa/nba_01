import time
import pandas as pd
from functools import wraps
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func
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

