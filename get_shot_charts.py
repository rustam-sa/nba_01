import time
import numpy as np
import pandas as pd
from nba_api.stats.endpoints import ShotChartDetail
from data_manager import DataManager


dm = DataManager()


def get_shot_chart_data(player_id, season):
    response = ShotChartDetail(
        player_id=player_id,
        team_id=0,  # 0 means fetch all teams
        season_nullable=season,
        context_measure_simple="FGA"
    )
    shot_data = response.get_data_frames()[0]
    return shot_data


def get_all_player_shot_charts():
    players = dm.query_players()
    player_names = [player.name for player in players]
    player_shot_charts = []
    
    for player_name in player_names:
        time.sleep(1)  # Avoid rate limiting
        player_shot_chart = get_player_shot_chart(player_name)
        player_shot_charts.append(player_shot_chart)
    
    # Combine all player shot charts into one DataFrame
    all_shot_charts = pd.concat(player_shot_charts, ignore_index=True)
    
    # Keep relevant columns
    relevant_columns = [
        'PLAYER_NAME', 'TEAM_NAME', 'LOC_X', 'LOC_Y', 'SHOT_ZONE_BASIC',
        'SHOT_ZONE_AREA', 'SHOT_ZONE_RANGE', 'SHOT_DISTANCE', 'SHOT_ATTEMPTED_FLAG',
        'SHOT_MADE_FLAG', 'DISTANCE'
    ]
    return all_shot_charts[relevant_columns]


def get_player_shot_chart(player_name):
    player_id = dm.get_player_nba_id(player_name)
    print("Got the player ID")
    shot_chart = get_shot_chart_data(player_id=player_id, season="2024-25")
    print("Got the shot chart")
    shot_chart['DISTANCE'] = np.sqrt(shot_chart['LOC_X']**2 + shot_chart['LOC_Y']**2)
    return shot_chart