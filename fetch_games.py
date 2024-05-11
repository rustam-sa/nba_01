from nba_api.stats.endpoints import leaguegamefinder
from datetime import datetime, timedelta


# Fetching data for the last 10 games for all teams
# def fetch_games():
# Assuming today's date minus the days to cover late games
date_from = datetime.now() - timedelta(days=30)  # Adjust as necessary

# Query all games from 'date_from' to today
game_finder = leaguegamefinder.LeagueGameFinder(date_from_nullable=date_from.strftime('%Y-%m-%d'))
games = game_finder.get_data_frames()  # Returns a DataFrame of games
print(games)

#     return games

# games_df = fetch_games()
# print(games_df)