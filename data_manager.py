import time
import itertools
import pandas as pd
import os
from datetime import date
from functools import wraps
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func, and_
from sqlalchemy.dialects.postgresql import insert
from requests.exceptions import HTTPError
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.endpoints import boxscoresummaryv2
from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.endpoints import boxscoreadvancedv2

import analyze
import date_utils as date_mng
from models import Team, Player, Game, TradTeamStats, AdvTeamStats, TradPlayerStats, AdvPlayerStats, TeamRollingAverages
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
                fga = 0 if stat_line['FGA'] is None else int(stat_line["FGA"])
                fg_pct = 0.0 if stat_line['FG_PCT'] is None else float(stat_line["FG_PCT"])
                fg3m = 0 if stat_line['FG3M'] is None else int(stat_line["FG3M"])
                fg3a = 0 if stat_line['FG3A'] is None else int(stat_line["FG3A"])
                fg3_pct = 0.0 if stat_line['FG3_PCT'] is None else float(stat_line["FG3_PCT"])
                ftm = 0 if stat_line['FTM'] is None else int(stat_line["FTM"])
                fta = 0 if stat_line['FTA'] is None else int(stat_line["FTA"])
                ft_pct = 0.0 if stat_line['FT_PCT'] is None else float(stat_line["FT_PCT"])
                oreb = 0 if stat_line['OREB'] is None else int(stat_line["OREB"])
                dreb = 0 if stat_line['DREB'] is None else int(stat_line["DREB"])
                reb = 0 if stat_line['REB'] is None else int(stat_line["REB"])
                ast = 0 if stat_line['AST'] is None else int(stat_line["AST"])
                stl = 0 if stat_line['STL'] is None else int(stat_line["STL"])
                blk = 0 if stat_line['BLK'] is None else int(stat_line["BLK"])
                to = 0 if stat_line['TO'] is None else int(stat_line["TO"])
                pf = 0 if stat_line['PF'] is None else int(stat_line["PF"])
                pts = 0 if stat_line['PTS'] is None else int(stat_line["PTS"])
                plus_minus = 0 if stat_line['PLUS_MINUS'] is None else int(stat_line["PLUS_MINUS"])
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
            # self.update_all_team_rolling_averages()
        
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
    def get_player_object(self, session, player_name):
        player = session.query(Player).filter(Player.name==player_name).all()[0]
        return player
    
    @session_management
    def get_player_team_id(self, session, player_name):
        player = session.query(Player).filter(Player.name==player_name).all()[0]
        team_id = player.team_id
        return team_id
    
    @session_management
    def get_team_id(self, session, team_nickname):
        team = session.query(Team).filter(Team.nickname==team_nickname).all()[0]
        team_id = team.id
        return team_id
    
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
                'game_id': game.id

            }
            data_list.append(row)
        if not data_list:
            return None
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
        if analyzed_props.empty:
            print("analyzed_props DataFrame is empty after filtering.")
            raise RuntimeError("analyzed_props DataFrame is empty after filtering.")
        
        filtered_df = analyzed_props.sort_values(by="PROB", ascending=False).head(n_props)
        filtered_df.to_csv(f"props_{date.today()}.csv")
        return filtered_df
    

    # @staticmethod
    # def filter_props(analyzed_props, filter_dict, top_n):
    #     print(len(analyzed_props))
    #     analyzed_props = [prop for prop in analyzed_props if prop.ev > 0]
    #     print(f"num of profitable props: {len(analyzed_props)}")
    #     analyzed_props = pd.DataFrame.from_dict([prop.entry for prop in analyzed_props])
    #     for key in filter_dict.keys():
    #         for filter_item, category in filter_dict[key]:
    #             print(f'Filtering {filter_item}')
    #             analyzed_props = analyzed_props[analyzed_props[category] != filter_item]

    #     print(len(analyzed_props))
    #     print(analyzed_props.head())
    #     raise
    #     filtered_df = analyzed_props.sort_values(by="PROB", ascending=False).head(top_n)

    #     filtered_df.to_csv(f"props_{date.today()}.csv")
    #     return filtered_df
    
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
            (TradTeamStats.team_id == team_id)
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
                'game_id': game.id,
                'pace' : adv_stats.pace,
                'def_rating': adv_stats.def_rating,
                'e_def_rating': adv_stats.e_def_rating,
                'off_rating': adv_stats.off_rating,
                'e_off_rating': adv_stats.e_off_rating,
            }
            data_list.append(row)

        data_df = pd.DataFrame(data_list)
        data_df = data_df.sort_values(by="date", ascending=False)
        save_destination = team.nickname if not filename else filename
        data_df.to_csv(f"data_pile/{save_destination}.csv")
        return data_df
    
    @staticmethod
    def create_pivot_table_for_tracking(df):
        pivot_df = df.assign(value=1).pivot_table(index=['PROP_TAG', 'THRESH'], columns='PARLAY_ID', values='value', fill_value="")

        # Reset index to move PROP_TAG and THRESH to columns
        pivot_df = pivot_df.reset_index()
        return pivot_df
    
    @session_management
    def get_all_team_stats(self, session):
        query = session.query(
            Game.id.label('game_id'),
            Game.date,
            TradTeamStats.team_id,
            Team.full_name.label('team_name'),
            TradTeamStats.minutes.label('trad_minutes'),
            TradTeamStats.fgm,
            TradTeamStats.fga,
            TradTeamStats.fg_pct,
            TradTeamStats.fg3m,
            TradTeamStats.fg3a,
            TradTeamStats.fg3_pct,
            TradTeamStats.ftm,
            TradTeamStats.fta,
            TradTeamStats.ft_pct,
            TradTeamStats.oreb,
            TradTeamStats.dreb,
            TradTeamStats.reb,
            TradTeamStats.ast,
            TradTeamStats.stl,
            TradTeamStats.blk,
            TradTeamStats.to,
            TradTeamStats.pf,
            TradTeamStats.pts,
            TradTeamStats.plus_minus,
            AdvTeamStats.minutes.label('adv_minutes'),
            AdvTeamStats.e_off_rating,
            AdvTeamStats.off_rating,
            AdvTeamStats.e_def_rating,
            AdvTeamStats.def_rating,
            AdvTeamStats.e_net_rating,
            AdvTeamStats.net_rating,
            AdvTeamStats.ast_pct,
            AdvTeamStats.ast_tov,
            AdvTeamStats.ast_ratio,
            AdvTeamStats.oreb_pct,
            AdvTeamStats.dreb_pct,
            AdvTeamStats.reb_pct,
            AdvTeamStats.e_tm_tov_pct,
            AdvTeamStats.tm_tov_pct,
            AdvTeamStats.efg_pct,
            AdvTeamStats.ts_pct,
            AdvTeamStats.usg_pct,
            AdvTeamStats.e_usg_pct,
            AdvTeamStats.e_pace,
            AdvTeamStats.pace,
            AdvTeamStats.pace_per40,
            AdvTeamStats.poss,
            AdvTeamStats.pie
        ).join(TradTeamStats, Game.id == TradTeamStats.game_id
        ).join(AdvTeamStats, (Game.id == AdvTeamStats.game_id) & (TradTeamStats.team_id == AdvTeamStats.team_id)
        ).join(Team, TradTeamStats.team_id == Team.id)

        df = pd.read_sql(query.statement, query.session.bind).sort_values(by=date, ascending=False)
        
        return df

    @staticmethod
    def get_team_averages(df):
        # Extract numeric columns
        numeric_df = df.select_dtypes(include='number')
        
        # Extract non-numeric columns (e.g., team_name)
        non_numeric_df = df[['team_name']].drop_duplicates()
        
        # Group by team_id and calculate mean for each group
        team_averages = numeric_df.groupby(df['team_name']).mean().reset_index()

        # Merge non-numeric columns back into the result if needed
        team_averages = team_averages.merge(non_numeric_df, left_on='team_name', right_on='team_name', how='left')
        
        return team_averages

    def get_all_team_averages(self):
        all_team_stats_df = self.get_all_team_stats()
        team_averages_df = self.get_team_averages(all_team_stats_df)
        return team_averages_df
    
    @staticmethod
    def create_directory(folder_name):
        """
        Creates a new directory with the specified folder name.

        Args:
            folder_name (str): The name of the folder to be created.

        Returns:
            str: The path of the created directory.
        """
        try:
            os.makedirs(folder_name, exist_ok=True)
            print(f"Directory '{folder_name}' created successfully.")
        except Exception as e:
            print(f"An error occurred while creating the directory: {e}")
        
        return os.path.abspath(folder_name)
    
    @staticmethod
    def save_as_excel_workbook(dataframes, file_name):
        writer = pd.ExcelWriter(f'{file_name}.xlsx', engine='openpyxl')
        for tag, df in dataframes.items():
            print(tag, df)
            df.to_excel(writer, sheet_name=tag)
        writer.close()

    @session_management
    def upsert_team_rolling_averages(self, session, record):
        insert_stmt = insert(TeamRollingAverages).values(
        points = record['points'],
        rebounds = record['rebounds'],
        assists = record['assists'],
        efg = record['efg'],
        fg3a = record['fg3a'],
        fg3m = record['fg3m'],
        fg3_pct = record['fg3_pct'],
        fga = record['fga'],
        fgm = record['fgm'],
        fta = record['fta'],
        ft_pct = record['ft_pct'],
        steals = record['steals'],
        blocks = record['blocks'],
        to = record['to'],  # turnovers
        pace = record['pace'],
        def_rating = record['def_rating'],
        e_def_rating = record['e_def_rating'],
        off_rating = record['off_rating'],
        e_off_rating = record['e_off_rating'],
        date = record['date'],
        game_id = record['game_id'],
        team_id = record['team_id'],
        )

        update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['game_id', 'team_id'],
            set_={
                "points" : record['points'],
                "rebounds" : record['rebounds'],
                "assists" : record['assists'],
                "efg" : record['efg'],
                "fg3a" : record['fg3a'],
                "fg3m" : record['fg3m'],
                "fg3_pct" :record['fg3_pct'],
                "fga" : record['fga'],
                "fgm" : record['fgm'],
                "fta" : record['fta'],
                "ft_pct" : record['ft_pct'],
                "steals" : record['steals'],
                "blocks" : record['blocks'],
                "to" : record['to'],  # turnovers
                "pace" : record['pace'],
                'def_rating' : record['def_rating'],
                "e_def_rating" : record['e_def_rating'],
                "off_rating" : record['off_rating'],
                "e_off_rating" : record['e_off_rating'],
                "date" : record['date'],
                "game_id" : record['game_id'],
                "team_id" : record['team_id'],
            }
        )

        session.execute(update_stmt)
        session.commit()

    def update_all_team_rolling_averages(self, average_method="median", window_size=10):
        teams = self.query_teams()
        team_ids = [team.id for team in teams]
        for team_id in team_ids:
            team_data = self.get_and_save_team_data(team_id)
            features = team_data.sort_values(by='date', ascending=True)
            feature_columns = ['points', 'rebounds', 'assists', 'efg', 'fg3a', 'fg3m', 'fg3_pct', 'fga', 'fgm', 'fta', 
                            'ft_pct', 'steals', 'blocks', 'to', 'pace', 'def_rating', 'e_def_rating', 'off_rating', 'e_off_rating']
            stats = features[feature_columns]
            if average_method == "median":
                rolling_averages = stats.shift(1).rolling(window=window_size).median()
            elif average_method == "mean":
                rolling_averages = stats.shift(1).rolling(window=window_size).mean()
            rolling_averages['date'] = features['date']
            rolling_averages['game_id'] = features['game_id']
            rolling_averages['team_id'] = team_id
            rolling_averages = rolling_averages.dropna()
            rolling_averages.to_csv("rolling_averages.csv")
            for _, record in rolling_averages.iterrows():
                record = (dict(record))
                self.upsert_team_rolling_averages(record)

    @session_management
    def count_records_in_table(self, session, model):
        count = session.query(func.count(model.id)).scalar()
        print(f'Total records: {count}')
        return count
    
    @session_management
    def get_team_rolling_stats(self, session, player_name):
        player = self.get_player_object(player_name)
        player_id = player.id
        team_id = player.team_id
        player_game_log = self.get_and_save_player_data(player_id)
        if player_game_log is None or player_game_log.empty:
            return None
        include_team_columns = ["date", "efg", "fg3a", "fg3_pct", "fga", "fta", "ft_pct", "steals", "blocks", "to", "pace", "e_def_rating", "e_off_rating"]
        include_opponent_columns = [col for col in include_team_columns if col != 'date']
        
        records = []
        for game_id in player_game_log.game_id:
            game_id = int(game_id)
            game = session.query(Game).filter_by(id=game_id).first()
            if team_id == game.home_team_id:
                opponent_id = game.away_team_id
            else:
                opponent_id = game.home_team_id
            team_rolling_averages = session.query(TeamRollingAverages).filter_by(game_id=game_id, team_id=team_id).first()
            if not team_rolling_averages:
                continue
            team_rolling_averages = {column.name: getattr(team_rolling_averages, column.name) for column in team_rolling_averages.__table__.columns}
            team_rolling_averages = pd.DataFrame([team_rolling_averages])
            team_rolling_averages = team_rolling_averages[include_team_columns]
            opponent_rolling_averages = session.query(TeamRollingAverages).filter_by(game_id=game_id, team_id=opponent_id).first()
            if not opponent_rolling_averages:
                continue
            opponent_rolling_averages = {column.name: getattr(opponent_rolling_averages, column.name) for column in opponent_rolling_averages.__table__.columns}
            opponent_rolling_averages = pd.DataFrame([opponent_rolling_averages])
            opponent_rolling_averages = opponent_rolling_averages[include_opponent_columns]
            opponent_rolling_averages.columns = ["opp_" + col for col in opponent_rolling_averages.columns]
            team_stats = pd.concat([team_rolling_averages, opponent_rolling_averages], axis=1)
            records.append(team_stats)
        return records

    @staticmethod
    def generate_parlays(df, min_props, max_props):
        parlays = []
        for r in range(min_props, max_props + 1):
            for combination in itertools.combinations(df.index, r):
                prob_product = 1
                house_prob_product = 1
                odds_product = 1
                ev_sum = 0
                for idx in combination:
                    prop_data = df[df.index == idx]
                    prob_product *= prop_data['PROB'].values[0]
                    odds_product *= analyze.american_to_decimal(prop_data['ODDS'].values[0])
                    house_prob_product *= prop_data['HOUSE_PROB'].values[0]
                    ev_sum += prop_data['EV'].values[0]
                    potential_winnings = 1 * (odds_product - 1)
                if ev_sum > min_props:
                    parlays.append({
                        'COMBO': combination,
                        'COMBINED_PROB': prob_product,
                        'COMBINED_HOUSE_PROB': house_prob_product,
                        'COMBINED_EV': ev_sum,
                        'TO_WIN': potential_winnings,

                    })
            if not parlays:
                raise RuntimeError("parlays is empty")
        return pd.DataFrame(parlays)

    def select_optimal_parlays(self, prop_df, max_permeation_rate, player_permeation_rate, min_props, max_props):
        """
        Selects optimal parlays based on given constraints.
        
        Args:
            prop_df (pd.DataFrame): DataFrame containing prop information with columns 'PLAYER', 'STAT', etc.
            max_permeation_rate (float): Maximum rate at which a prop can be permeated.
            player_permeation_rate (float): Maximum rate at which a player can be permeated.
            min_props (int): Minimum number of props in a parlay.
            max_props (int): Maximum number of props in a parlay.
        
        Returns:
            list: List of selected parlays.
        """
        
        # Generate all possible parlays
        parlays_df = self.generate_parlays(prop_df, min_props, max_props)

        # Sort parlays by combined expected value
        parlays_df = parlays_df.sort_values(by="COMBINED_EV", ascending=False).reset_index(drop=True)
        # Assign unique ID to each parlay
        parlays_df['PARLAY_ID'] = parlays_df.index
        
        # Calculate the maximum number of parlays using the updated calculation
        num_parlays = self.calculate_num_parlays(prop_df, max_permeation_rate, min_props, max_props)
        
        # Initialize counts
        parlays_selected = []
        prop_counts = {prop: 0 for prop in prop_df.index}
        player_counts = {player: 0 for player in prop_df['PLAYER']}
        
        # Select parlays
        for _, parlay in parlays_df.iterrows():
            can_add_parlay = True
            seen = {}
            
            for idx in parlay['COMBO']:
                prop_row = prop_df.iloc[idx]
                player = prop_row['PLAYER']
                stat = prop_row['STAT']

                # Check for conflicts in stat types
                if stat == 'points':
                    if player in seen and 'fgm' in seen[player]:
                        can_add_parlay = False
                        break
                elif stat == 'fgm':
                    if player in seen and 'points' in seen[player]:
                        can_add_parlay = False
                        break

                # Check for prop permeation limit
                if prop_counts[idx] >= num_parlays * max_permeation_rate:
                    can_add_parlay = False
                    break

                # Check for player permeation limit
                if player_counts[player] >= num_parlays * player_permeation_rate:
                    can_add_parlay = False
                    break

                if player in seen:
                    seen[player] += [stat]
                else:
                    seen[player] = [stat]
            
            if can_add_parlay:
                parlays_selected.append(parlay)
                for idx in parlay['COMBO']:
                    prop_counts[idx] += 1
                for player in seen:
                    player_counts[player] += 1

        return parlays_selected
    
    @staticmethod
    def calculate_num_parlays(prop_df, max_permeation_rate, min_props, max_props):
        """
        Calculate the optimal number of parlays based on given constraints.
        
        Args:
            prop_df (pd.DataFrame): DataFrame containing prop information.
            max_permeation_rate (float): Maximum rate at which a prop can be permeated.
            min_props (int): Minimum number of props in a parlay.
            max_props (int): Maximum number of props in a parlay.
            
        Returns:
            int: Optimal number of parlays.
        """
        
        # Calculate the average parlay size
        avg_parlay_size = (min_props + max_props) / 2
        
        # Adjust the calculation to consider the average parlay size
        num_parlays = int(len(prop_df) / (avg_parlay_size * max_permeation_rate))
        
        return num_parlays
    
    @staticmethod
    def get_prop_distribution(parlays):
        appearance_counts = parlays['PROP_TAG'].value_counts()
        parlay_count = len(parlays['PARLAY_ID'].unique())
        percentages = pd.Series(appearance_counts/parlay_count)
        parlay_distribution = pd.concat([appearance_counts, percentages], axis=1)
        parlay_distribution.columns = ['COUNT', '%']
        return parlay_distribution

    @staticmethod
    def get_player_distribution(parlays):
        appearance_counts = parlays.groupby('PLAYER')['PARLAY_ID'].nunique().reset_index()
        appearance_counts.columns = ['PLAYER', 'PARLAY_COUNT']
        parlay_count = len(parlays['PARLAY_ID'].unique())
        percentages = pd.Series(appearance_counts['PARLAY_COUNT']/parlay_count)
        parlay_distribution = pd.concat([appearance_counts, percentages], axis=1)
        parlay_distribution.columns = ['PLAYER', 'PARLAY_COUNTS', '%']
        parlay_distribution = parlay_distribution.sort_values(by="PARLAY_COUNTS", ascending=False)
        return parlay_distribution

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

