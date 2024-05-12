from datetime import datetime, timedelta


def get_date_n_days_ago(n):
    today = datetime.now()
    ten_days_ago = today - timedelta(days=n)
    formatted_date = ten_days_ago.strftime("%m/%d/%Y")
    return formatted_date

def get_todays_date():
    today = datetime.now()
    formatted_date = today.strftime("%m/%d/%Y")
    return formatted_date

