import logging
from _decimal import Decimal

from demeter import RowData, MarketDict, UniLpMarket

from strategies.brokkr_strategy import BrokkrStrategy
from utils.ui_utils import parse_floats_array


class BrokkrOverlappingStrategy(BrokkrStrategy):
    @staticmethod
    def name():
        return "Overlapping strategy - center ranges"

    @staticmethod
    def get_arguments():
        return [{
            "name": "Ranges",
            "example": "2,5",
            "description": "Comma separated ranges.",
            "parser": parse_floats_array
        }, {
            "name": "Ranges distribution",
            "example": "1,2 - If passed ranges are 1,3,6 and ranges_distribution are 1,2,2 "
                       "it means investing 1/5 of funds into 1%, and 2/5 of funds into ranges 3% and 6%.",
            "description": "Comma-separated ranges for weight distribution. "
                           "The count of ranges_distribution must be the same as the ranges. "
                           "The first weight refers to the first range, and so on",
            "parser": parse_floats_array
        }]

    # noinspection PyDefaultArgument
    def __init__(self, ranges=[3, 5, 10], ranges_distribution=[1, 1, 1]):
        assert len(ranges) == len(ranges_distribution)
        super().__init__()
        self.ranges = ranges
        self.ranges_distribution = ranges_distribution

    def initialize_custom(self):
        market: UniLpMarket = self.markets.default
        init_price = market.market_status.price
        weights_sum = sum(self.ranges_distribution)
        self.symmetrical_rebalance(market, init_price)

        base_holdings, quote_holdings = self.get_base_and_quote_holdings()

        for i, range in enumerate(self.ranges):
            weight = Decimal(self.ranges_distribution[i] / weights_sum)
            self.add_liquidity(market, range,
                               base_max_amount=base_holdings * weight,
                               quote_max_amount=quote_holdings * weight)

        self.print_assets()

    def on_bar_custom(self, row_data: MarketDict[RowData]):
        if not len(self.account_status):
            return

        market: UniLpMarket = self.markets.default
        current_price = market.market_status.price
        timestamp = market.market_status.timestamp

        for position_info, position in list(self.markets.default.positions.items()):
            l, h = self.get_low_high_borders(position_info)
            is_position_out_of_range = not (l < current_price < h)

            if is_position_out_of_range:
                logging.info(f"({timestamp}) Got out of range. ")
                self.remove_liquidity(market, position_info)
                self.symmetrical_rebalance(market, current_price)
                self.add_liquidity(market, position.range)

        self.set_ranges_for_chart()

    def set_ranges_for_chart(self):
        widest_position = max(list(self.markets.default.positions.keys()),
                              key=lambda x: abs(x.lower_tick - x.upper_tick))
        l, h = self.get_low_high_borders(widest_position)

        self.current_range_low = l
        self.current_range_high = h
