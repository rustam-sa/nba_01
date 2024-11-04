import os
import pandas as pd
from data_manager import DataManager

dm = DataManager()


def extract_raw_data(file_path): # .csv
    # gets input from A1
    # Sample input text (use the content of your file here)
    raw_input = pd.read_csv(file_path)
    list_of_raw_input = list(raw_input.iloc[:, 0])
    return list_of_raw_input


def load_available_props(file_path):
    
    raw_input = extract_raw_data(file_path)
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
    players = dm.query_players()
    player_names = [player.name for player in players]
    teams = dm.query_teams()
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

    return df


def process_all_csv_files_in_directory(directory):
    combined_records = []
    
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            print(file_path)
            df = load_available_props(file_path)
            combined_records.append(df)

    # Combine all dataframes into one if desired
    final_df = pd.concat(combined_records, ignore_index=True)
    return final_df