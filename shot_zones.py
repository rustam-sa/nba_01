from nba_api.stats.endpoints import LeagueDashPlayerShotLocations
import pandas as pd



def aggregate_shot_zones(seasons:list, season_type="Regular Season"):
    # Fetch shot zone data
    shot_zone_dfs = []
    for season in seasons:
        shot_zone_data = LeagueDashPlayerShotLocations(
            season=season,
            season_type_all_star=season_type
        )
        shot_zone_df = shot_zone_data.get_data_frames()[0]
        shot_zone_df.columns = [' '.join(col).strip() if isinstance(col, tuple) else col for col in shot_zone_df.columns]
        shot_zone_dfs.append(shot_zone_df)

    shot_zone_combined = pd.concat(shot_zone_dfs)
    shot_zone_combined = shot_zone_combined.groupby(['PLAYER_NAME'], as_index=False).sum()
    return shot_zone_combined