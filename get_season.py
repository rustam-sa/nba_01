from nba_api.stats.endpoints import leaguegamefinder
import pandas as pd

def get_season_start_and_end(season):
    if len(season) == 9:
        season = season[:5] + season[-2:]
# Initialize the LeagueGameFinder with desired parameters
    game_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season, league_id_nullable='00')

    # Convert the games to a DataFrame
    games = game_finder.get_data_frames()[0]

    # Convert game dates to datetime and find the earliest date
    games['GAME_DATE'] = pd.to_datetime(games['GAME_DATE'])
    start_date = games['GAME_DATE'].min()
    end_date = games['GAME_DATE'].max()

    print(f"{season} season's first game: {start_date.date()}")
    print(f"The {season} season's last game: {end_date.date()}")

    return start_date, end_date

test = get_season_start_and_end("2023-2024")
print(test)
