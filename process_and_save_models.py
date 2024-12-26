# %% Imports
from data_manager import DataManager
import pandas as pd
import numpy as np
import pickle
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# %% Prepare player data
def prepare_player_data(player_data):
    """
    Prepares data for regression modeling for an individual player.

    Parameters:
        player_data (pd.DataFrame): DataFrame containing game-level data for one player.

    Returns:
        pd.DataFrame, pd.Series: Features and target variable.
    """
    # Ensure the data is sorted by date
    player_data = player_data.sort_values('date')

    # Create lagged features
    player_data['rolling_avg_points'] = player_data['points'].shift(1).rolling(window=5).mean()
    player_data['rolling_avg_minutes'] = player_data['minutes'].shift(1).rolling(window=5).mean()
    player_data['rolling_avg_rebounds'] = player_data['rebounds'].shift(1).rolling(window=5).mean()

    # Remove rows where lagged features are NaN (e.g., first few games)
    player_data = player_data.dropna()

    # Define features and target
    features = player_data[['rolling_avg_points', 'rolling_avg_minutes', 'rolling_avg_rebounds', 'date']]
    target = player_data['points']

    return features, target


# %% Train a model for a player
def train_player_model(features, target):
    """
    Trains a regression model for an individual player.

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

    # Train a Random Forest Regressor
    model = RandomForestRegressor(random_state=42)
    model.fit(X_train, y_train)

    # Evaluate the model
    predictions = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))

    return model, rmse


# %% Save trained models
def save_models(models, output_dir='production_models'):
    """
    Saves trained models to disk.

    Parameters:
        models (dict): Dictionary of trained models for players.
        output_dir (str): Directory to save the models.
    """
    os.makedirs(output_dir, exist_ok=True)

    for player, data in models.items():
        model_path = os.path.join(output_dir, f"{player.replace(' ', '_')}_model.pkl")
        with open(model_path, 'wb') as f:
            pickle.dump(data['model'], f)
        print(f"Model for {player} saved. RMSE: {data['rmse']:.2f}")


# %% Train models for all players and save them
def process_and_save_models(players, dm, output_dir='production_models'):
    """
    Processes data, trains models, and saves results for all players.

    Parameters:
        players (list): List of player names.
        dm (DataManager): DataManager instance for fetching data.
        output_dir (str): Directory to save the models.

    Returns:
        dict: Dictionary containing models and RMSEs for each player.
    """
    all_models = {}
    os.makedirs(output_dir, exist_ok=True)

    for player in players:
        print(f"Processing {player}...")
        try:
            # Fetch player data
            player_id = dm.get_player_id(player)
            data = dm.get_and_save_player_data(player_id)

            # Prepare features and target
            features, target = prepare_player_data(data)

            if len(features) == 0:
                print(f"Not enough data for {player}, skipping...")
                continue

            # Train the model
            model, rmse = train_player_model(features, target)

            # Save the model to the production directory
            model_path = os.path.join(output_dir, f"{player.replace(' ', '_')}_model.pkl")
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
            print(f"Model for {player} saved to {model_path}. RMSE: {rmse:.2f}")

            # Store the model and RMSE
            all_models[player] = {'model': model, 'rmse': rmse}

        except Exception as e:
            print(f"Error processing {player}: {e}")

    return all_models


# %% Example usage
if __name__ == "__main__":
    # Initialize DataManager
    dm = DataManager()

    # Fetch all players (assuming this returns a list of player names)
    players = [player.name for player in dm.query_players()]

    # Train models and save them to the production directory
    all_models = process_and_save_models(players, dm, output_dir='production_models')

    # Analyze results
    for player, info in all_models.items():
        print(f"Player: {player}, RMSE: {info['rmse']:.2f}")
