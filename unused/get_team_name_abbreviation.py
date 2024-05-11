import json

with open('nicknames_to_abbreviations_map.json', 'r') as file:
    abbreviations_map = json.load(file)

def return_abbreviation(partial_team_nickname):
    partial_team_nickname = partial_team_nickname.lower().strip()
    for full_name, abbreviation in abbreviations_map.items():
        if partial_team_nickname in full_name.lower():
            return abbreviation
    return "No matching team found"