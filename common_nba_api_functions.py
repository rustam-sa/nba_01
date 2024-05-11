import time
import pandas as pd
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.library.parameters import SeasonAll

def rate_limited_request(interval=10):
    """Pauses the execution to ensure it doesn't hit the API rate limit."""
    time.sleep(interval)


def get_gamelog_from_years(player_id, years):
    years = ", ".join([str(year) for year in years])
    gamelog = pd.concat(playergamelog.PlayerGameLog(player_id=player_id, season=SeasonAll.all).get_data_frames())
    gamelog["GAME_DATE"] = pd.to_datetime(gamelog["GAME_DATE"], format="%b %d, %Y")
    gamelog = gamelog.query(f"GAME_DATE.dt.year in [{years}]")
    return gamelog


def get_season_start_and_end(season):
    if len(season) == 9:
        season = season[:5] + season[-2:]
    game_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season, league_id_nullable='00')

    games = game_finder.get_data_frames()[0]

    games['GAME_DATE'] = pd.to_datetime(games['GAME_DATE'])
    start_date = games['GAME_DATE'].min()
    end_date = games['GAME_DATE'].max()
    print(f"{season} season's first game: {start_date.date()}")
    print(f"The {season} season's last game: {end_date.date()}")
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    return start_date, end_date