import os
from datetime import datetime, timedelta

def get_date_n_days_ago(n):
        today = datetime.now()
        date_n_days_ago = today - timedelta(days=n)
        return date_n_days_ago.strftime("%m/%d/%Y")

def get_todays_date():
        today = datetime.now()
        return today.strftime("%m/%d/%Y")

def format_date(date_obj):
        return date_obj.strftime("%m/%d/%Y")

def subtract_days(date, n):
    return date - timedelta(days=n)

def get_or_create_directory():
    # Get today's date in YYYY-MM-DD format
    today_date = datetime.today().strftime('%Y-%m-%d')
    # Define the directory name
    directory_name = today_date

    # Get the current working directory
    current_directory = os.getcwd()
    # Define the path to the new directory
    path_to_directory = os.path.join(current_directory, directory_name)

    # Check if the directory already exists
    if not os.path.exists(path_to_directory):
        # Create the directory if it does not exist
        os.makedirs(path_to_directory)
        print(f"Directory created: {path_to_directory}")
    else:
        print(f"Directory already exists: {path_to_directory}")

    # Return the path to the directory
    return path_to_directory

def get_or_create_directory_in_days():
    # Get today's date in YYYY-MM-DD format
    today_date = datetime.today().strftime('%Y-%m-%d')
    # Define the directory name
    directory_name = today_date

    # Get the current working directory
    current_directory = os.getcwd()
    # Define the path to the "days" directory
    days_directory = os.path.join(current_directory, 'days')

    # Create the "days" directory if it does not exist
    if not os.path.exists(days_directory):
        os.makedirs(days_directory)
        print(f"Directory created: {days_directory}")
    else:
        print(f"Directory already exists: {days_directory}")

    # Define the path to the new directory within the "days" directory
    path_to_directory = os.path.join(days_directory, directory_name)

    # Check if the directory already exists
    if not os.path.exists(path_to_directory):
        # Create the directory if it does not exist
        os.makedirs(path_to_directory)
        print(f"Directory created: {path_to_directory}")
    else:
        print(f"Directory already exists: {path_to_directory}")

    # Return the path to the directory
    return path_to_directory