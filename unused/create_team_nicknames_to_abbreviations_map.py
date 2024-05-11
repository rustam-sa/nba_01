import json
from nba_api.stats.static import teams

nba_teams = teams.get_teams()
nicknames = [dic["nickname"] for dic in nba_teams]
abbreviations = [dic["abbreviation"] for dic in nba_teams]
nicknames_to_abbreviations_dict = dict(zip(nicknames, abbreviations))


def update_key(old_key, new_nicknames):
    if old_key in nicknames_to_abbreviations_dict:
        abbreviation = nicknames_to_abbreviations_dict.pop(old_key)  # Remove the old key
        new_key = f"{old_key} {new_nicknames}"  # Create new key
        nicknames_to_abbreviations_dict[new_key] = abbreviation  # Assign the abbreviation to the new key
    else:
        print("Key not found.")


additional_nicknames = [("76ers", "sixers")]
for old_nickname, new_nickname in additional_nicknames:
    update_key(old_nickname, new_nickname)

with open('nicknames_to_abbreviations_map.json', 'w') as file:
    json.dump(nicknames_to_abbreviations_dict, file)