import hashlib
from _decimal import Decimal
from typing import Dict, List, Tuple

import pandas as pd
from demeter import Actuator
from demeter._typing import PositionInfo, TokenInfo
from pandas import Timestamp

from strategies.brokkr_strategy import BrokkrStrategy


def string_to_hex_color(s: str) -> str:
    hash_object = hashlib.md5(s.encode())
    hash_hex = hash_object.hexdigest()
    color = '#' + hash_hex[:6]
    return color


def construct_multiple_position_custom_columns(strategy: BrokkrStrategy, base_token: TokenInfo, actuator: Actuator) -> list:
    positions_low_ranges: Dict[PositionInfo, List[Tuple[Timestamp, Decimal]] | pd.Series] = dict()
    positions_high_ranges: Dict[PositionInfo, List[Tuple[Timestamp, Decimal]] | pd.Series] = dict()

    for ranges_low in strategy.range_low_line_list:
        for position_info, range_low, timestamp in ranges_low:
            if position_info in positions_low_ranges:
                positions_low_ranges[position_info].append((timestamp, range_low))
            else:
                positions_low_ranges[position_info] = [(timestamp, range_low)]

    for ranges_high in strategy.range_high_line_list:
        for position_info, range_high, timestamp in ranges_high:
            if position_info in positions_high_ranges:
                positions_high_ranges[position_info].append((timestamp, range_high))
            else:
                positions_high_ranges[position_info] = [(timestamp, range_high)]

    multiple_positions_custom_data = [
        {"label": f'Initial holdings in {base_token.name}',
         'color': "c-",
         'linewidth': 1,
         "data": actuator.broker.markets.default.data.initial_investment_value,
         'ax': 'value'}
    ]

    for position_low_item, position_high_item in zip(list(positions_low_ranges.items()), list(positions_high_ranges.items())):
        position_info_low, position_low_ranges_list = position_low_item
        position_info_high, position_high_ranges_list = position_high_item
        color = string_to_hex_color(str(position_info_low))

        idx_low, values_low = zip(*position_low_ranges_list)
        positions_low_ranges[position_info_low] = pd.Series(pd.Series(values_low, idx_low), index=actuator.broker.markets.default.data.index)
        multiple_positions_custom_data.append({
            "data": positions_low_ranges[position_info_low],
            'ax': 'price',
            "color": color
         })

        idx_high, values_high = zip(*position_high_ranges_list)
        positions_high_ranges[position_info_high] = pd.Series(pd.Series(values_high, idx_high), index=actuator.broker.markets.default.data.index)
        multiple_positions_custom_data.append({
            "data": positions_high_ranges[position_info_high],
            'ax': 'price',
            "color": color
        })

    return multiple_positions_custom_data
