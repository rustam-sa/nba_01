import pandas as pd
from scipy.stats import poisson


def estimate_probability_poisson_over(data, stat, n):
    mean = data[stat].mean()
    probability = 1 - poisson.cdf(n, mean)
    return probability

def estimate_probability_poisson_under(data, stat, n):
    mean = data[stat].mean()
    probability = poisson.cdf(n, mean)  # Calculate P(X <= n)
    return probability

def estimate_implied_probability(decimal_odds):
    """Convert decimal odds to implied probability."""
    return 1 / decimal_odds

# def calculate_ev(probability, decimal_odds, bet_amount):
#     """Calculate the expected value (EV) of a bet.

#     Args:
#         probability (float): The probability of the event occurring (between 0 and 1).
#         decimal_odds (float): The decimal odds for the bet.
#         bet_amount (float): The amount wagered.

#     Returns:
#         float: The expected value (EV) of the bet.
#     """
#     payout = decimal_odds * bet_amount
#     ev = (probability * payout) - bet_amount
#     return ev

def calculate_ev(probability, decimal_odds, bet_amount):
    """Calculate the expected value (EV) of a bet.

    Args:
        probability (float): The probability of the event occurring (between 0 and 1).
        decimal_odds (float): The decimal odds for the bet.
        bet_amount (float): The amount wagered.

    Returns:
        float: The expected value (EV) of the bet.
    """
    net_profit = (decimal_odds - 1) * bet_amount
    ev = (probability * net_profit) - (1 - probability) * bet_amount
    return ev


def calculate_combined_probability(probabilities):
    """Calculate the combined probability for a parlay."""
    combined_probability = 1
    for prob in probabilities:
        combined_probability *= prob
    return combined_probability


def calculate_combined_odds(american_odds_list):
    """Calculate the combined decimal odds for a parlay."""
    decimal_odds_list = [american_to_decimal(odds) for odds in american_odds_list]
    combined_odds = 1
    for odds in decimal_odds_list:
        combined_odds *= odds
    return combined_odds    


def analyze_parlay(probabilities, odds_list, bet_amount):
    """
    Analyze a parlay bet and print the combined probability, combined odds, expected value, and potential winnings.

    Args:
        probabilities (list of float): The probabilities of individual bets.
        american_odds_list (list of int): The American odds for individual bets.
        bet_amount (float): The amount wagered.
    """
    combined_probability = calculate_combined_probability(probabilities)
    combined_odds = calculate_combined_odds(odds_list)
    ev = calculate_ev(combined_probability, combined_odds, bet_amount)
    
    potential_winnings = bet_amount * (combined_odds - 1)

    print(f"Combined Probability: {combined_probability:.4f}")
    print(f"Combined Odds: {combined_odds:.2f}")
    print(f"Expected Value (EV): ${ev:.2f}")
    print(f"Potential Winnings: ${potential_winnings:.2f}")

    return ev, potential_winnings


def american_to_decimal(american_odds):
    """Convert American odds to decimal odds."""
    if american_odds > 0:
        return 1 + (american_odds / 100)
    else:
        return 1 + (100 / abs(american_odds))
    

def analyze_parlays(parlays):
    result = []
    remove_these = []
    for i, parlay in enumerate(parlays):
        probabilities = [prop['PROB'] for prop in parlay]
        odds = [prop['ODDS'] for prop in parlay]
        house_probabilities = [prop['HOUSE_PROB'] for prop in parlay]
        combined_ev, potential_winnings = analyze_parlay(probabilities, odds, 1)
        combined_prob = calculate_combined_probability(probabilities)
        combined_house_prob = calculate_combined_probability(house_probabilities)
        seen = {}


        for prop in parlay:
            if prop['STAT'] == 'points':
                if prop['PLAYER'] in seen.keys():
                    if seen[prop['PLAYER']] == 'fgm':
                        remove_these.append(i)
                        break
            if prop['STAT'] == 'fgm':
                if prop['PLAYER'] in seen.keys():
                    if seen[prop['PLAYER']] == 'points':
                        remove_these.append(i)
                        break
            seen[prop['PLAYER']] = prop['STAT']
            prop['PARLAY_ID'] = i
            prop['PARLAY_PROB'] = combined_prob
            prop['HOUSE_PARLAY_PROB'] = combined_house_prob
            prop['PARLAY_EV'] = combined_ev
            prop['TO_WIN'] = potential_winnings
        parlay_df = pd.DataFrame(parlay)
        result.append(parlay_df)
    

    for index in sorted(remove_these, reverse=True):
        del result[index]
    parlays_df = pd.concat(result, ignore_index=True)
    return parlays_df