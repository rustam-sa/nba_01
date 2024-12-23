# %%
from data_manager import DataManager
from utils import open_dataframe_in_temp_excel
import pandas as pd
import numpy as np
import pickle
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import root_mean_squared_error


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
    player_data['rolling_avg_points'] = player_data['points'].shift(1).rolling(window=5).mean()
    player_data['rolling_avg_minutes'] = player_data['minutes'].shift(1).rolling(window=5).mean()
    player_data['rolling_avg_rebounds'] = player_data['rebounds'].shift(1).rolling(window=5).mean()

    # Remove rows where lagged features are NaN (e.g., first few games)
    player_data = player_data.dropna()

    # Define features and target
    features = player_data[['rolling_avg_points', 'rolling_avg_minutes', 'rolling_avg_rebounds', 'date']]
    target = player_data['points']

    return features, target


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
    # data = data.sort_values('date')

    # Calculate the split index
    split_idx = int(len(data) * (1 - test_size))

    # Split features and target
    X_train = data.iloc[:split_idx].drop(columns=['date'])
    X_test = data.iloc[split_idx:].drop(columns=['date'])
    y_train = target.iloc[:split_idx]
    y_test = target.iloc[split_idx:]

    return X_train, X_test, y_train, y_test


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
    X_train, X_test, y_train, y_test = temporal_train_test_split(features, target, test_size=0.2)

    # Train a Random Forest Regressor
    model = RandomForestRegressor(random_state=42)
    model.fit(X_train, y_train)

    # Evaluate the model
    predictions = model.predict(X_test)
    rmse = np.sqrt(root_mean_squared_error(y_test, predictions))

    return model, rmse


def train_models_for_all_players(data):
    """
    Trains regression models for all players in the dataset.

    Parameters:
        data (pd.DataFrame): Combined DataFrame containing all players' game data.

    Returns:
        dict: Dictionary of player names and their trained models.
    """
    players = data['player_name'].unique()
    models = {}

    for player in players:
        print(f"Training model for {player}...")
        player_data = data[data['player_name'] == player]
        features, target = prepare_player_data(player_data)

        if len(features) > 0:  # Ensure there's enough data to train
            model, rmse = train_player_model(features, target)
            models[player] = {
                'model': model,
                'rmse': rmse
            }
            print(f"Model for {player} trained. RMSE: {rmse:.2f}")
        else:
            print(f"Not enough data for {player}, skipping...")

    return models


def shuffle_test(features, target):
    """
    Performs a shuffle test to detect potential data leakage.

    Parameters:
        features (pd.DataFrame): Feature matrix.
        target (pd.Series): Target variable.

    Returns:
        float: RMSE of the model with shuffled target.
    """
    # Shuffle the target
    shuffled_target = target.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # Perform train-test split
    X_train, X_test, y_train, y_test = temporal_train_test_split(features, shuffled_target, test_size=0.2)
    
    # Train the model
    model = RandomForestRegressor(random_state=42)
    model.fit(X_train, y_train)
    
    # Evaluate the model
    predictions = model.predict(X_test)
    rmse = np.sqrt(np.mean((y_test - predictions) ** 2))
    return rmse


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


dm = DataManager()
players = dm.query_players()  # Assuming this returns a list of player names

def process_all_players(players, dm, output_dir='player_models'):
    """
    Process data, train models, and save results for all players.

    Parameters:
        players (list): List of player names.
        dm (DataManager): DataManager instance for fetching data.
        output_dir (str): Directory to save the models and results.

    Returns:
        dict: Dictionary containing models and RMSEs for each player.
    """
    os.makedirs(output_dir, exist_ok=True)  # Ensure output directory exists
    all_models = {}

    for player in players:
        player_name = player.name
        print(f"Processing {player_name}...")
        try:
            # Fetch player data
            player_id = dm.get_player_id(player_name)
            data = dm.get_and_save_player_data(player_id)

            # Prepare features and target
            features, target = prepare_player_data(data)

            # Check if there's enough data
            if len(features) == 0:
                print(f"Not enough data for {player_name}, skipping...")
                continue

            # Train the model
            model, rmse = train_player_model(features, target)

            # Save model to disk
            model_path = f"{output_dir}/{player_name.replace(' ', '_')}_model.pkl"
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
            print(f"Model for {player_name} saved. RMSE: {rmse:.2f}")

            # Store results
            all_models[player_name] = {'model': model, 'rmse': rmse}

        except Exception as e:
            print(f"Error processing {player_name}: {e}")

    return all_models

# %% Run the process for all players
all_models = process_all_players(players, dm)

# %% Analyze results
for player, info in all_models.items():
    print(f"Player: {player}, RMSE: {info['rmse']:.2f}")

# %% Example prediction for a player
def predict_for_all_players(features, models_dir='player_models'):
    """
    Predict for all players based on their saved models.

    Parameters:
        features (pd.DataFrame): Feature matrix for the prediction.
        models_dir (str): Directory where models are saved.

    Returns:
        dict: Predictions for all players.
    """
    predictions = {}
    for player in os.listdir(models_dir):
        if player.endswith('.pkl'):
            player_name = player.replace('_model.pkl', '').replace('_', ' ')
            with open(f"{models_dir}/{player}", 'rb') as f:
                model = pickle.load(f)
            predictions[player_name] = model.predict(features)
    return predictions

# %% Example usage for predictions
# Assuming `features` is prepared for the next game for all players
# predictions = predict_for_all_players(features)
# print(predictions)
