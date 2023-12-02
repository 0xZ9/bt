import re
from _decimal import Decimal
from datetime import datetime
from os import walk
from typing import Optional, List

from demeter import TokenInfo

from strategies.brokkr_strategy import BrokkrStrategy


def all_subclasses(cls):
    return set(cls.__subclasses__()).union([s for c in cls.__subclasses__() for s in all_subclasses(c)])


def get_user_input(pool_address, data_path, base_token: TokenInfo):
    start_date, end_date = get_ranges(pool_address, data_path)
    initial_base_token_amount = get_initial_base_token_amount(base_token)
    str_input = get_strategy_and_input()

    return start_date, end_date, initial_base_token_amount, str_input


def get_strategy_and_input():
    strategies: list[BrokkrStrategy] = list(all_subclasses(BrokkrStrategy))
    strategies.sort(key=lambda s: s.name())

    header = "\nPlease pick a strategy (available strategies: https://www.notion.so/brokkr-finance/Uniswap-v3-Liquidity-providing-Internal-testing-ff8b22912aa94d2284cc3c9945cb5294?pvs=4#0948154164864056a8631469575d6d05)\n"
    query_string = ""
    for i, strategy in enumerate(strategies):
        query_string += f"{i + 1}. {strategy.name()}\n"

    while True:
        print(header)
        user_input = input(query_string)
        if user_input.isdigit() and (int(user_input) > 0 and int(user_input) <= len(strategies)):
            strategy = strategies[int(user_input) - 1]
            break

    arguments_to_return = []
    for arguments in strategy.get_arguments():
        while True:
            user_input = input(
                f"Please set {arguments['name']}\n{arguments['description']} (example: {arguments['example']})\n")
            result = arguments['parser'](user_input)
            if result is not None:
                arguments_to_return.append(result)
                break

    return strategy, arguments_to_return


def get_strategy_type():
    while True:
        print(
            "\nPlease pick a strategy (available strategies: https://www.notion.so/brokkr-finance/Uniswap-v3-Liquidity-providing-Internal-testing-ff8b22912aa94d2284cc3c9945cb5294?pvs=4#0948154164864056a8631469575d6d05)")
        strategy = input(
            ""
            "1. Invest all with a constant range\n"
            "2. Invest with strategy A\n"
            "3. Invest with strategy B\n"
            "4. Invest with strategy C\n")

        if strategy.isdigit() and (int(strategy) > 0 and int(strategy) < 5):
            break

    return strategy


def get_pool_from_user(available_pools: [str]) -> str:
    print("Please pick a pool:")

    pool_number = 1

    for pool in available_pools:
        print(f'{pool_number}. {pool}')
        pool_number += 1

    while True:
        user_input = input(f"Type: 1-{len(available_pools)}: ")

        if user_input.isdigit() and 1 <= int(user_input) <= len(available_pools):
            break

    return available_pools[int(user_input) - 1]


def get_ranges(pool_address, data_path):
    date_format = '%d-%m-%Y'
    filenames = next(walk(data_path), (None, None, []))[2]

    pattern = rf"Arbitrum-{pool_address.lower()}-(\d{{4}}-\d{{2}}-\d{{2}})\.csv"

    matching_dates = []
    for filename in filenames:
        if not filename.startswith("raw") and filename.endswith(".csv"):
            match = re.search(pattern, filename)
            if match:
                date = match.group(1)
                matching_dates.append(datetime.fromisoformat(date))

    max_date = max(matching_dates)
    min_date = min(matching_dates)

    print(
        f"Please pick a backtesting range between {min_date.strftime(date_format)} {max_date.strftime(date_format)}")

    range_from = None
    range_to = None

    while True:
        user_input = input("From: (example: 29-01-2023)\n")

        try:
            range_from = datetime.strptime(user_input, date_format)
        except:
            pass

        if range_from and max_date >= range_from >= min_date:
            break

    while True:
        user_input = input("To: (example: 29-01-2023)\n")

        try:
            range_to = datetime.strptime(user_input, '%d-%m-%Y')
        except:
            pass

        if range_to and max_date >= range_to >= min_date and range_to > range_from:
            break

    return range_from, range_to


def get_initial_base_token_amount(base_token: TokenInfo):
    while True:
        initial_base_token_amount = input(f'Please specify initial {base_token.name} holdings (example: 100)\n')

        if initial_base_token_amount.isdigit() and int(initial_base_token_amount) > 0 and int(
                initial_base_token_amount):
            break

    return initial_base_token_amount


def parse_floats_array(ranges: bytes):
    ranges_numbers = []

    try:
        ranges = ranges.split(sep=",")
        for r in ranges:
            ranges_numbers.append(float(r))
    except:
        return None

    return ranges_numbers


def parse_decimals_array(ranges: bytes) -> Optional[List[Decimal]]:
    ranges_numbers = []

    try:
        ranges = ranges.split(sep=",")
        for r in ranges:
            ranges_numbers.append(Decimal(r))
    except:
        return None

    return ranges_numbers


def parse_integers_array(bytes: bytes):
    numbers = []

    try:
        for num in bytes.split(sep=","):
            if num.isdigit():
                numbers.append(int(num))
            else:
                return None
    except:
        return None

    return numbers


def parse_positive_integer(bytes: bytes):
    if bytes.isdigit() and int(bytes) > 0:
        return int(bytes)


def parse_positive_float(bytes: bytes):
    try:
        if float(bytes) > 0:
            return float(bytes)
    except:
        return None


def parse_positive_decimal(bytes: bytes) -> Optional[Decimal]:
    try:
        if float(bytes) > 0:
            return Decimal(str(bytes))
    except:
        return None


def parse_out_of_range_threshold(bytes: bytes):
    float_number = parse_positive_float(bytes)

    if float_number is not None and 100 >= float_number > 0:
        return float_number
