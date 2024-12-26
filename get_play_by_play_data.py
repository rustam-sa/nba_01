from nba_api.stats.endpoints import PlayByPlay 
from data_manager import DataManager
dm = DataManager()


def get_play_by_play_data(game_id):

    def fetch_play_by_play(nba_game_id):
        try:
            # Fetch data using nba_api
            pbp_data = PlayByPlay(game_id=nba_game_id)
            # Convert to DataFrame
            pbp_df = pbp_data.get_data_frames()[0]
            return pbp_df
        except Exception as e:
            print(f"Error fetching play-by-play data: {e}")
            return None

    
    def format_nba_game_id(nba_game_id):
        nba_game_id = str(nba_game_id)
        count_of_zeroes = 10 - len(nba_game_id)
        nba_game_id = count_of_zeroes * "0" + nba_game_id
        return nba_game_id

    nba_game_id = format_nba_game_id(dm.get_nba_game_id(game_id))
    return fetch_play_by_play(nba_game_id)