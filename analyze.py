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

def calculate_ev(probability, decimal_odds, bet_amount):
    """Calculate the expected value (EV) of a bet.

    Args:
        probability (float): The probability of the event occurring (between 0 and 1).
        decimal_odds (float): The decimal odds for the bet.
        bet_amount (float): The amount wagered.

    Returns:
        float: The expected value (EV) of the bet.
    """
    payout = decimal_odds * bet_amount
    ev = (probability * payout) - bet_amount
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
    Analyze a parlay bet and print the combined probability, combined odds, and expected value.

    Args:
        probabilities (list of float): The probabilities of individual bets.
        american_odds_list (list of int): The American odds for individual bets.
        bet_amount (float): The amount wagered.
    """
    combined_probability = calculate_combined_probability(probabilities)
    combined_odds = calculate_combined_odds(odds_list)
    ev = calculate_ev(combined_probability, combined_odds, bet_amount)
    
    print(f"Combined Probability: {combined_probability:.4f}")
    print(f"Combined Odds: {combined_odds:.2f}")
    print(f"Expected Value (EV): ${ev:.2f}")

    return ev


def american_to_decimal(american_odds):
    """Convert American odds to decimal odds."""
    if american_odds > 0:
        return 1 + (american_odds / 100)
    else:
        return 1 + (100 / abs(american_odds))