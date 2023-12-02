from _decimal import Decimal
from datetime import timedelta

from demeter import UniLpMarket
from pandas import Timestamp
from numpy import sqrt

from constants import PERCENT_TO_DECIMAL, ONE
from models.rebalance_direction import RebalanceDirection


def timestamp_to_key(timestamp: Timestamp):
    year = timestamp.year
    dayofyear = timestamp.dayofyear
    return f"{year}-{dayofyear}"


def is_one_day_or_older(historical_timestamp, current_timestamp):
    one_day = timedelta(days=1)
    time_difference = current_timestamp - historical_timestamp
    return time_difference >= one_day


def get_price_change_in_percentage(price_before, price_now):
    if price_before == 0 or price_now == 0:
        return 0

    price_change = price_now - price_before
    price_change_percentage = (price_change / price_before) * 100
    return price_change_percentage


def get_lower_upper_price_for_symmetric_range(range_in_percentage: Decimal, price: Decimal):
    # assure Decimal
    range_in_percentage = Decimal(range_in_percentage)
    price = Decimal(price)

    range = PERCENT_TO_DECIMAL * range_in_percentage
    lower_quote_price = price / Decimal(ONE + range)
    upper_quote_price = price * Decimal(ONE + range)

    return lower_quote_price, upper_quote_price


def get_lower_upper_price_for_asymmetric_range(range_dwn_in_percentage: Decimal, range_up_in_percentage: Decimal,
                                               price: Decimal):
    # normalise percentage in decimal
    range_downside = PERCENT_TO_DECIMAL * range_dwn_in_percentage
    range_upside = PERCENT_TO_DECIMAL * range_up_in_percentage

    lower_quote_price = price * Decimal(ONE - range_downside)
    upper_quote_price = price * Decimal(ONE + range_upside)

    return lower_quote_price, upper_quote_price


# Inspired by: https://ethereum.stackexchange.com/questions/99425/calculate-deposit-amount-when-adding-to-a-liquidity-pool-in-uniswap-v3
def calculate_base_diff_for_asymmetric_position(base_token_price: Decimal, base_token_balance, total_capital_in_base_token: Decimal,
                                                lower_base_price: Decimal, upper_base_price: Decimal):
    liquidity_base = (ONE * sqrt(base_token_price) * sqrt(upper_base_price)) / (sqrt(upper_base_price) - sqrt(base_token_price))

    quote_asset_per_1_base = liquidity_base * (sqrt(base_token_price) - sqrt(lower_base_price))

    base_amount_to_convert_in_quote = (quote_asset_per_1_base * total_capital_in_base_token) / (base_token_price + quote_asset_per_1_base)

    return total_capital_in_base_token - base_amount_to_convert_in_quote - base_token_balance



def get_rebalance_direction(position, market: UniLpMarket, current_price: Decimal) -> RebalanceDirection:
    price_low = market.tick_to_price(position.lower_tick)
    price_high = market.tick_to_price(position.upper_tick)

    diff_down = abs(current_price - price_low)
    diff_up = abs(current_price - price_high)

    if diff_up < diff_down:
        return RebalanceDirection.UP
    else:
        return RebalanceDirection.DOWN


# Note: this is custom rebalance function derived from Dementor "market.even_rebalance"
def market_asymmetric_rebalance(market: UniLpMarket, rebalancing_price: Decimal,
                                quote_token_price: Decimal = None) -> (Decimal, Decimal, Decimal):
    """
    Divide assets equally between two tokens based on the rebalancing price (derived from get_asymmetric_rebalancing_price())

    :param rebalancing_price: rebalancing price (i.e. ratio) which determines distribution of assets
    :type rebalancing_price: Decimal
    :param quote_token_price: price of quote token. eg: 1234 usdc/eth
    :type quote_token_price: Decimal
    :return: fee, base token amount spend, quote token amount got
    :rtype: (Decimal, Decimal, Decimal)
    """
    if quote_token_price is None:
        quote_token_price = market._market_status.price

    total_capital = market.broker.get_token_balance(market.base_token) + market.broker.get_token_balance(
        market.quote_token) * quote_token_price
    target_base_amount = total_capital / 2
    quote_amount_diff = target_base_amount / rebalancing_price - market.broker.get_token_balance(market.quote_token)
    if quote_amount_diff > 0:
        return market.buy(quote_amount_diff)
    elif quote_amount_diff < 0:
        return market.sell(0 - quote_amount_diff)
