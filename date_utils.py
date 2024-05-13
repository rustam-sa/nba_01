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
