import logging
from _decimal import Decimal

from demeter import RowData, MarketDict

from strategies.brokkr_strategy import BrokkrStrategy
from utils.strategy_utils import timestamp_to_key
from utils.ui_utils import parse_positive_integer, parse_positive_decimal


class BrokkrStrategyA(BrokkrStrategy):
    @staticmethod
    def name():
        return "Strategy A"

    @staticmethod
    def get_arguments():
        return [{
            "name": "Range (in percentage)",
            "example": "2",
            "description": "",
            "parser": parse_positive_decimal
        }, {
            "name": "max rebalances per day",
            "example": "3",
            "description": "",
            "parser": parse_positive_integer
        }]

    day_to_rebalance_count = {}

    def __init__(self, range_in_percentage=Decimal(5), max_rebalances_per_day=3):
        super().__init__()
        self.range_in_percentage = range_in_percentage
        self.max_rebalances_per_day = max_rebalances_per_day

    def initialize_custom(self):
        self.rebalance_and_add_symmetric_liquidity(self.range_in_percentage)

    def on_bar_custom(self, row_data: MarketDict[RowData]):
        if not len(self.account_status):
            return

        if self.is_out_of_range():
            current_account_status = self.account_status[-1]
            timestamp = current_account_status.timestamp
            key = timestamp_to_key(timestamp)
            rebalanced_count = self.day_to_rebalance_count.setdefault(key, 0)
            if rebalanced_count >= self.max_rebalances_per_day:
                logging.info(
                    f"({timestamp}) Skipping rebalance, reached max rebalances: {self.max_rebalances_per_day}")
            else:
                self.rebalance_and_add_symmetric_liquidity(self.range_in_percentage)
                self.day_to_rebalance_count[key] = rebalanced_count + 1
