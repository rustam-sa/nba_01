# %%
from data_manager import DataManager
from utils import open_dataframe_in_temp_excel

# %%
dm = DataManager()

# %%
player_name = "LeBron James"

player_id = dm.get_player_id(player_name)
data = dm.get_and_save_player_data(player_id)

# %%
def get_team_log(full_team_name):
    team_id = dm.get_team_id_by_full_name(full_team_name)
    team_game_data = dm.get_and_save_team_data(team_id)
    return team_game_data

# %%
import pandas as pd


def add_opponent_features_with_era(player_game_log, span=10):
    """
    Adds opponent team features to each game record in the player's game log using exponential rolling averages.
    
    Parameters:
    - player_game_log (pd.DataFrame): DataFrame of player's game log.
    - team_game_logs (dict): Dictionary of team_name -> pd.DataFrame of game logs for all teams.
    - span (int): Span for the exponential rolling averages (default=10).
    
    Returns:
    - pd.DataFrame: Updated player's game log with opponent features.
    """
    features = []

    for _, record in player_game_log.iterrows():
        opponent_team = record['team_name']  # Or other column identifying the opponent
        game_date = pd.to_datetime(record['date'])
        
        # Fetch opponent's game log
        opponent_log = get_team_log(opponent_team)

        if opponent_log.empty:
            # If no data for the opponent, add NaNs or defaults
            features.append({
                'opp_era_def_rating': None,
                'opp_era_pace': None,
                'opp_era_reb': None,
                'opp_games_count': 0
            })
            continue

        # Filter opponent's games up to the current game date
        opponent_log = opponent_log[pd.to_datetime(opponent_log['game_date']) < game_date]
        
        if opponent_log.empty:
            # No prior games available
            features.append({
                'opp_era_def_rating': None,
                'opp_era_pace': None,
                'opp_era_reb': None,
                'opp_games_count': 0
            })
            continue
        
        opponent_log = opponent_log.sort_values(by='game_date', ascending=True)

        # Calculate exponential rolling averages
        era_def_rating = opponent_log['def_rating'].ewm(span=span, adjust=False).mean().iloc[-1]
        era_pace = opponent_log['pace'].ewm(span=span, adjust=False).mean().iloc[-1]
        era_reb = opponent_log['reb'].ewm(span=span, adjust=False).mean().iloc[-1]
        games_count = len(opponent_log)

        # Append calculated features
        features.append({
            'opp_era_def_rating': era_def_rating,
            'opp_era_pace': era_pace,
            'opp_era_reb': era_reb,
            'opp_games_count': games_count
        })

    # Convert features to a DataFrame
    features_df = pd.DataFrame(features)

    # Concatenate the features with the player's game log
    enriched_game_log = pd.concat([player_game_log.reset_index(drop=True), features_df], axis=1)

    return enriched_game_log

# Example usage:
# player_game_log = pd.read_csv("player_game_log.csv")  # Load the player's game log
# get_team_game_log = lambda team: pd.read_csv(f"data/{team}_game_log.csv")  # Example team log loader
# enriched_log = add_opponent_features(player_game_log, get_team_game_log)


# %%
player_game_data = add_opponent_features_with_era(data)

# %%
def prepare_player_data(player_data):
    """
    Prepares data for regression modeling for an individual player, avoiding target leakage.

    Parameters:
        player_data (pd.DataFrame): DataFrame containing game-level data for one player.

    Returns:
        pd.DataFrame, pd.Series: Features and target variable.
    """
    # Ensure the data is sorted by date
    player_data = player_data.sort_values('date')

    # Create lagged features
    player_data['rolling_avg_points'] = player_data['points'].shift(1).rolling(window=15).mean()
    player_data['rolling_avg_minutes'] = player_data['minutes'].shift(1).rolling(window=15).mean()
    player_data['rolling_avg_rebounds'] = player_data['rebounds'].shift(1).rolling(window=15).mean()

    # Remove rows where lagged features are NaN (e.g., first few games)
    player_data = player_data.dropna()

    # Define features and target
    features = player_data[['opp_era_pace', 'rolling_avg_minutes', "rolling_avg_points"]]
    target = player_data['points']

    return features, target



# %%
def temporal_train_test_split(data, target, test_size=0.2):
    """
    Splits the data into training and testing sets based on time order.

    Parameters:
        data (pd.DataFrame): Feature matrix including a 'date' column.
        target (pd.Series): Target variable.
        test_size (float): Proportion of data to use for the test set.

    Returns:
        X_train, X_test, y_train, y_test: Split features and targets.
    """
    # Sort data by date
    data = data.sort_values('date')

    # Calculate the split index
    split_idx = int(len(data) * (1 - test_size))

    # Split features and target
    X_train = data.iloc[:split_idx].drop(columns=['date'])
    X_test = data.iloc[split_idx:].drop(columns=['date'])
    y_train = target.iloc[:split_idx]
    y_test = target.iloc[split_idx:]

    return X_train, X_test, y_train, y_test


# %%
# import numpy as np
# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.metrics import root_mean_squared_error


# def train_player_model(features, target):
#     """
#     Trains a regression model for an individual player.

#     Parameters:
#         features (pd.DataFrame): Feature matrix.
#         target (pd.Series): Target variable (e.g., points).

#     Returns:
#         model: Trained regression model.
#         float: RMSE of the model on the test set.
#     """
#     # Temporal train-test split
#     X_train, X_test, y_train, y_test = temporal_train_test_split(features, target, test_size=0.2)

#     # Train a Random Forest Regressor
#     model = RandomForestRegressor(random_state=42)
#     model.fit(X_train, y_train)

#     # Evaluate the model
#     predictions = model.predict(X_test)
#     rmse = root_mean_squared_error(y_test, predictions)

#     return model, rmse



# %%
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error

def train_player_model(features, target):
    """
    Trains a regression model for an individual player using XGBoost.

    Parameters:
        features (pd.DataFrame): Feature matrix.
        target (pd.Series): Target variable (e.g., points).

    Returns:
        model: Trained regression model.
        float: RMSE of the model on the test set.
    """
    # Temporal train-test split
    split_idx = int(len(features) * 0.8)
    X_train, X_test = features.iloc[:split_idx], features.iloc[split_idx:]
    y_train, y_test = target.iloc[:split_idx], target.iloc[split_idx:]

    # Train an XGBoost Regressor
    model = XGBRegressor(
        n_estimators=100,       # Number of trees
        learning_rate=0.33,      # Step size shrinkage
        max_depth=3,            # Maximum tree depth
        subsample=0.5,          # Subsample ratio of the training set
        colsample_bytree=0.8,   # Subsample ratio of columns per tree
        eval_metric="rmse",     # Evaluation metric
        early_stopping_rounds=100, 
        random_state=42,         # For reproducibility
    )
    model.fit(
        X_train, 
        y_train,
        eval_set=[(X_test, y_test)], 
 
        verbose=False
    )

    # Evaluate the model
    predictions = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))

    return model, rmse


def train_models_for_all_players(data):
    """
    Trains regression models for all players in the dataset using XGBoost.

    Parameters:
        data (pd.DataFrame): Combined DataFrame containing all players' game data.

    Returns:
        dict: Dictionary of player names and their trained models.
    """
    players = data['player_name'].unique()
    models = {}

    for player in players:
        print(f"Training model for {player}...")
        try:
            # Filter data for the player
            player_data = data[data['player_name'] == player]
            features, target = prepare_player_data(player_data)

            if len(features) > 0:  # Ensure there's enough data to train
                # Train the model
                model, rmse = train_player_model(features, target)

                # Store the model and its performance
                models[player] = {
                    'model': model,
                    'rmse': rmse
                }
                print(f"Model for {player} trained. RMSE: {rmse:.2f}")
            else:
                print(f"Not enough data for {player}, skipping...")

        except Exception as e:
            print(f"Error training model for {player}: {e}")

    return models


# %%
models = train_models_for_all_players(player_game_data)

# %%
def baseline_rmse(target, test_size=0.2):
    """
    Computes the RMSE of a baseline model that predicts the mean of the training target.

    Parameters:
        target (pd.Series): Target variable.

    Returns:
        float: Baseline RMSE.
    """
    # Split target
    split_idx = int(len(target) * (1 - test_size))
    y_train = target.iloc[:split_idx]
    y_test = target.iloc[split_idx:]

    # Predict mean of training target
    baseline_prediction = np.mean(y_train)
    rmse = np.sqrt(np.mean((y_test - baseline_prediction) ** 2))
    return rmse

# Compute baseline RMSE
baseline_rmse_value = baseline_rmse(target)
print(f"Baseline RMSE (predicting mean): {baseline_rmse_value:.2f}")


# %%
model = models[player_name]['model']

# %%
feature_importances = model.feature_importances_
importance_df = pd.DataFrame({
    'Importance': feature_importances
}).sort_values(by='Importance', ascending=False)

print(importance_df)

# %%
features, target = prepare_player_data(player_game_data)
X_train, X_test, y_train, y_test = temporal_train_test_split(features, target)

# %%
predictions = model.predict(X_test)

# %%
import pandas as pd
results = pd.DataFrame([np.array(predictions), y_test]).T
results.columns = ['PREDS', "Y_TEST"]
results['INDEX'] = np.arange(1, len(results) + 1)

# %%
open_dataframe_in_temp_excel(results)

# %%
display(results)


# %%
importances = model.feature_importances_
sorted_importances = sorted(zip(importances, X_train.columns), reverse=True)
for importance, feature in sorted_importances:
    print(f"{feature}: {importance:.4f}")


# %%
import pickle

def save_models(models, output_dir='player_models'):
    """
    Saves trained models to disk.

    Parameters:
        models (dict): Dictionary of trained models for players.
        output_dir (str): Directory to save the models.
    """
    import os
    os.makedirs(output_dir, exist_ok=True)

    for player, data in models.items():
        with open(f"{output_dir}/{player.replace(' ', '_')}_model.pkl", 'wb') as f:
            pickle.dump(data['model'], f)
        print(f"Model for {player} saved.")


# %%
def predict_player_performance(player, features, model_dir='player_models'):
    """
    Predicts performance difference for a player.

    Parameters:
        player (str): Player's name.
        features (pd.DataFrame): Feature matrix for the upcoming game.
        model_dir (str): Directory containing saved models.

    Returns:
        np.ndarray: Predicted performance differences.
    """
    model_path = f"{model_dir}/{player.replace(' ', '_')}_model.pkl"

    with open(model_path, 'rb') as f:
        model = pickle.load(f)

    return model.predict(features)



