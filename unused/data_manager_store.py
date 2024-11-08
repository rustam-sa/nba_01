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
from models import Team, Player # , #Game, PlayerStats, TeamStats
from db_config import get_database_engine, get_session

       
class DataManager:

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



    def get_session(self):
        return get_session()
    
    def get_engine(self):
        return get_database_engine()
    
    def query_latest_team_stats_date(self):
        session = get_session()
        try:
            latest_date = session.query(func.max(Game.date)).\
                        join(TeamStats, TeamStats.game_id == Game.id).\
                        scalar()
            return latest_date
        
        finally:
            session.close()

    @session_management
    def query_latest_player_stats_date(self, session, player):
            latest_date = session.query(func.max(Game.date)).\
                        join(PlayerStats, PlayerStats.game_id == Game.id).\
                        filter(PlayerStats.player_id == player.id).\
                        scalar()
            return latest_date
    

    @session_management
    def query_player_gamelog(self, session, player):
        # Perform the query
        gamelog_query = session.query(PlayerStats, Game.date.label("game_date"))\
                            .join(Game, PlayerStats.game_id == Game.id)\
                            .filter(PlayerStats.player_id == player.player_id)\
                            .options(joinedload(PlayerStats.game))  # Ensure efficient loading of related Game data
        # Convert query results to a list of dictionaries
        gamelog_list = [{
            **game_stats.to_dict(),  # Assuming you have a method to_dict() in PlayerStats or you can use __dict__
            "game_date": game_date
        } for game_stats, game_date in gamelog_query]
        
        # Create DataFrame from list of dictionaries
        gamelog_df = pd.DataFrame(gamelog_list)
        
        return gamelog_df

    def pull_player_gamelog_starting_from(self, player, date=None):
        if not date:
            date = date_mng.get_date_n_days_ago(1)

        else:
            date = date_mng.format_date(date)
        
        gamelog = pd.concat(
            playergamelog.PlayerGameLog(
                player_id=player.player_id,
                season_type_all_star="Playoffs",
                date_from_nullable=date, 
                ).get_data_frames())
        gamelog["GAME_DATE"] = pd.to_datetime(gamelog["GAME_DATE"], format="%b %d, %Y")
        gamelog["PLAYER_ID"] = player.player_id
        return gamelog
    
    @session_management
    def update_player_game_stats(self, session, player):
        try:
            last_game_date = self.query_latest_game_stats_date()
            last_game_date_minus_one_day = date_mng.subtract_days(last_game_date, 1)
            new_games = self.pull_player_gamelog_starting_from(last_game_date_minus_one_day)

            records = []
            for _, game in new_games.iterrows():
                print(f"Adding new game stat for game ID: {game['Game_ID']}")
                new_game_stat = PlayerStats(
                    player_id=game['PLAYER_ID'],
                    game_id=game['Game_ID'],
                    points=game['PTS'],
                    assists=game['AST'],
                    rebounds=game['REB'],
                    steals=game['STL'],
                    blocks=game['BLK'],
                    turnovers=game['TOV'],
                    fouls=game['PF'],
                    minutes=game['MIN'],
                    fg_made=game['FGM'],
                    fg_attempts=game['FGA'],
                    fg_percentage=game['FG_PCT'],
                    fg3_made=game['FG3M'],
                    fg3_attempts=game['FG3A'],
                    fg3_percentage=game['FG3_PCT'],
                    ft_made=game['FTM'],
                    ft_attempts=game['FTA'],
                    ft_percentage=game['FT_PCT'],
                    plus_minus=game['PLUS_MINUS']
                )
                records.append(new_game_stat)
            session.add_all(records)
            session.commit()
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            session.rollback()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            session.close()
    
    def pull_team_games_starting_from(self, date=None):
        if not date:
            date = date_mng.get_date_n_days_ago(1)

        else:
            date = date_mng.format_date(date)
        
        teams = self.query_teams()
        games = []
        for team in teams:
            gamelog = leaguegamefinder.LeagueGameFinder(
                team_id_nullable=team.team_id, 
                date_from_nullable=date
                ).get_data_frames()
            gamelog["GAME_DATE"] = pd.to_datetime(gamelog["GAME_DATE"], format="%b %d, %Y")
            gamelog["TEAM_ID"] = team.team_id
            games.append(gamelog)
        games = pd.concat(games, axis=0)
        return games
    
    @session_management
    def update_team_games(self, session):
        try:
            last_game_date = self.query_latest_team_stats_date()
            last_game_date_minus_one_day = date_mng.subtract_days(last_game_date, 1)
            new_games = self.pull_team_games_starting_from(last_game_date_minus_one_day)

            records = []
            for index, row in games.iterrows():
                game_date = datetime.strptime(row['GAME_DATE'], '%Y-%m-%d')

                # Check if the game already exists
                game = session.query(Game).filter_by(id=row['GAME_ID']).first()
                if not game:
                    game = Game(id=row['GAME_ID'], date=game_date,
                                home_team_id=row['TEAM_ID'] if 'vs.' in row['MATCHUP'] else None,
                                away_team_id=row['TEAM_ID'] if '@' in row['MATCHUP'] else None)
                    session.add(game)

                # Check if TeamStats already exists for this game and team
                team_stats = session.query(TeamStats).filter_by(game_id=row['GAME_ID'], team_id=row['TEAM_ID']).first()
                if not team_stats:
                    # Create new TeamStats if none exist
                    team_stats = TeamStats(
                        game=game,
                        team_id=row['TEAM_ID']
                    )
                    session.add(team_stats)

                # Update existing or new TeamStats with current data
                team_stats.points=row['PTS']
                team_stats.field_goals_made=row['FGM']
                team_stats.field_goals_attempted=row['FGA']
                team_stats.field_goal_percentage=row['FG_PCT']
                team_stats.three_point_field_goals_made=row['FG3M']
                team_stats.three_point_field_goals_attempted=row['FG3A']
                team_stats.three_point_field_goal_percentage=row['FG3_PCT']
                team_stats.free_throws_made=row['FTM']
                team_stats.free_throws_attempted=row['FTA']
                team_stats.free_throw_percentage=row['FT_PCT']
                team_stats.offensive_rebounds=row['OREB']
                team_stats.defensive_rebounds=row['DREB']
                team_stats.total_rebounds=row['REB']
                team_stats.assists=row['AST']
                team_stats.steals=row['STL']
                team_stats.blocks=row['BLK']
                team_stats.turnovers=row['TOV']
                team_stats.personal_fouls=row['PF']
                team_stats.plus_minus=row['PLUS_MINUS']
            session.add_all(games)
            session.commit()
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            session.rollback()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            session.close()

    @staticmethod
    def pull_teams():
        nba_teams = teams.get_teams()
        return nba_teams

    @session_management
    def update_team_record(self, session, team):
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

    def update_all_team_records(self):
        teams = self.pull_teams()
        for team in teams:
            self.update_team_record(team)

    def update_all_player_records(self):
        players = self.pull_players()
        for _, player_row in players.iterrows():
            player = player_row.to_dict()
            self.update_player_record(player)
        
    @session_management
    def query_players(self, session):
        return session.query(Player).all()
    
    @session_management
    def query_teams(self, session):
        return session.query(Team).all()
    
    @session_management
    def query_games(self, session):
        return session.query(Game).all()
    
    @session_management
    def query_player_game_stats(self, session):
        pass

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
    
    @session_management
    def update_player_record(self, session, player):
        if not isinstance(player, dict):
            raise TypeError(f"Expected player to be a dictionary, got {type(player)} instead.")

        insert_stmt = insert(Player).values(
            nba_player_id=player["PLAYER_ID"],
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

    @staticmethod
    def pull_game_advanced_stats():
        pass

    @staticmethod
    def pull_games_by_team_and_season(team, season, season_type):
        try:
            gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team.nba_team_id, season_nullable=season, season_type_nullable=season_type)
            games_df = gamefinder.get_data_frames()[0]
            return games_df

        except Exception as e:
            print(f"An error occurred: {e}")
            print(gamefinder.get_json())

    # @session_management
    # xdef update_team_game_records(self, session, games):
    #     for index, row in games.iterrows():
    #         game_date = datetime.strptime(row['GAME_DATE'], '%Y-%m-%d')

    #         # Upsert Game
    #         insert_stmt = insert(Game).values(
    #             nba_game_id = row["NBA_GAME_ID"],
    #             date=game_date,
    #             season=row['SEASON'],
    #             season_type=row["SEASON_TYPE"],
    #             home_team_id= row['TEAM_ID']if 'vs.' in row['MATCHUP'] else None,
    #             away_team_id=row['TEAM_ID'] if '@' in row['MATCHUP'] else None
    #         )

    #         do_update_stmt = insert_stmt.on_conflict_do_update(
    #             index_elements=['nba_game_id'],
    #             set_={
    #                 'date': game_date,
    #                 'season': row['SEASON'],
    #                 'season_type': row["SEASON_TYPE"],
    #                 'home_team_id': row['TEAM_ID'] if 'vs.' in row['MATCHUP'] else None,
    #                 'away_team_id': row['TEAM_ID'] if '@' in row['MATCHUP'] else None
    #             }
    #         )

    #         result = session.execute(do_update_stmt)
        
    #         # If the game already exists, get its ID
    #         game_id = result.inserted_primary_key[0] if result.is_insert else session.query(Game).filter_by(
    #             date=game_date,
    #             season=row['SEASON'],
    #             season_type=row["SEASON_TYPE"],
    #             home_team_id=row['TEAM_ID'] if 'vs.' in row['MATCHUP'] else None,
    #             away_team_id=row['TEAM_ID'] if '@' in row['MATCHUP'] else None
    #         ).first().id

    #         # Upsert TeamStats
    #         insert_stmt = insert(TeamStats).values(
    #             game_id=game_id,
    #             points=row['PTS'],
    #             field_goals_made=row['FGM'],
    #             field_goals_attempted=row['FGA'],
    #             field_goal_percentage=row['FG_PCT'],
    #             three_point_field_goals_made=row['FG3M'],
    #             three_point_field_goals_attempted=row['FG3A'],
    #             three_point_field_goal_percentage=row['FG3_PCT'],
    #             free_throws_made=row['FTM'],
    #             free_throws_attempted=row['FTA'],
    #             free_throw_percentage=row['FT_PCT'],
    #             offensive_rebounds=row['OREB'],
    #             defensive_rebounds=row['DREB'],
    #             total_rebounds=row['REB'],
    #             assists=row['AST'],
    #             steals=row['STL'],
    #             blocks=row['BLK'],
    #             turnovers=row['TOV'],
    #             personal_fouls=row['PF'],
    #             plus_minus=row['PLUS_MINUS']
    #         )

    #         do_update_stmt = insert_stmt.on_conflict_do_update(
    #             index_elements=['game_id'],
    #             set_={
    #                 'points': row['PTS'],
    #                 'field_goals_made': row['FGM'],
    #                 'field_goals_attempted': row['FGA'],
    #                 'field_goal_percentage': row['FG_PCT'],
    #                 'three_point_field_goals_made': row['FG3M'],
    #                 'three_point_field_goals_attempted': row['FG3A'],
    #                 'three_point_field_goal_percentage': row['FG3_PCT'],
    #                 'free_throws_made': row['FTM'],
    #                 'free_throws_attempted': row['FTA'],
    #                 'free_throw_percentage': row['FT_PCT'],
    #                 'offensive_rebounds': row['OREB'],
    #                 'defensive_rebounds': row['DREB'],
    #                 'total_rebounds': row['REB'],
    #                 'assists': row['AST'],
    #                 'steals': row['STL'],
    #                 'blocks': row['BLK'],
    #                 'turnovers': row['TOV'],
    #                 'personal_fouls': row['PF'],
    #                 'plus_minus': row['PLUS_MINUS']
    #             }
    #         )

    #         session.execute(do_update_stmt)

    #     session.commit()
    @staticmethod
    def pull_game_summary(game_id):
            game_summary = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
            game_summary_df = game_summary.game_summary.get_data_frame()
            return game_summary_df

    @staticmethod
    def pull_traditional_stats_for_game(game_id):
        boxscore_traditional = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        
        player_stats = boxscore_traditional.player_stats.get_data_frame()
        team_stats = boxscore_traditional.team_stats.get_data_frame()
        
        return player_stats, team_stats
    
    @staticmethod
    def pull_advanced_stats_for_game(game_id):

        boxscore_advanced = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
        

        player_stats = boxscore_advanced.player_stats.get_data_frame()
        team_stats = boxscore_advanced.team_stats.get_data_frame()
        
        return player_stats, team_stats
    
