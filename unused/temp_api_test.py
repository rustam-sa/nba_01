import requests

# Example URL for testing - adjust parameters as needed
url = "https://stats.nba.com/stats/leaguegamefinder?DateFrom=2023-05-01&DateTo=2023-05-10"
headers = {'User-Agent': 'Mozilla/5.0'}
response = requests.get(url, headers=headers)
print(response.json())