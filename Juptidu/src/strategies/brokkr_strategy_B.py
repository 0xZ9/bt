import logging
from datetime import timedelta
from _decimal import Decimal

from demeter import RowData, MarketDict
from pandas import Timestamp

from strategies.brokkr_strategy import BrokkrStrategy
from utils.strategy_utils import timestamp_to_key, is_one_day_or_older
from utils.ui_utils import parse_floats_array, parse_integers_array


class BrokkrStrategyB(BrokkrStrategy):
    day_to_rebalance_count = {}
    max_range_active_since: Timestamp = Timestamp(0)

    @staticmethod
    def name():
        return "Strategy B"

    @staticmethod
    def get_arguments():
        return [{
            "name": "Ranges",
            "example": "2,5",
            "description": "Comma separated ranges. When first rebalance level is reached,"
                           " next one is being used. Any count of ranges is allowed.",
            "parser": parse_floats_array
        }, {
            "name": "max rebalances per day",
            "example": "2,5 - passing ranges=1,3,6 and rebalances=1,2,3 means 1 rebalance with range 1%, "
                       "2 rebalances with 3% and 3 rebalances with 6% range until the end of the day.",
            "description": "Comma separated rebalances, count of rebalances must be the same as ranges, first rebalance"
                           " level refers to first first range, and so on.",
            "parser": parse_integers_array
        }]

    # noinspection PyDefaultArgument
    def __init__(self, ranges=[2, 5], max_rebalances=[3, 2]):
        assert len(ranges) == len(max_rebalances)
        super().__init__()
        self.ranges = ranges
        self.max_rebalances = max_rebalances

    def initialize_custom(self):
        self.rebalance_and_add_symmetric_liquidity(Decimal(self.ranges[0]))

    def on_bar_custom(self, row_data: MarketDict[RowData]):
        if not len(self.account_status) or not self.can_rebalance():
            return

        self.handle_current_timestamp()

    def handle_current_timestamp(self):
        current_timestamp: Timestamp = self.account_status[-1].timestamp
        widest_range_active_since_1d = self.max_range_active_since.timestamp() > 0 and \
                                       is_one_day_or_older(self.max_range_active_since, current_timestamp)
        if widest_range_active_since_1d:
            logging.info(
                f"({current_timestamp}) Rebalancing and resetting rebalances count because widest range is since {self.max_range_active_since}")
            self.rebalance_and_add_symmetric_liquidity(Decimal(self.ranges[0]))

            key = timestamp_to_key(current_timestamp)
            self.max_range_active_since = Timestamp(0)
            self.day_to_rebalance_count[key] = 0
        elif self.is_out_of_range():
            self.on_rebalance_needed(current_timestamp)

    def on_rebalance_needed(self, timestamp: Timestamp):
        key = timestamp_to_key(timestamp)
        rebalanced_count = self.day_to_rebalance_count.setdefault(key, 0)

        range_for_rebalance = -1
        max_rebalances_sum = 0
        for i, range in enumerate(self.ranges):
            max_rebalances_for_this_range = self.max_rebalances[i]
            max_rebalances_sum += max_rebalances_for_this_range

            if rebalanced_count < max_rebalances_sum:
                range_for_rebalance = range
                break

        if range_for_rebalance < 0:
            logging.info(
                f"({timestamp}) Skipping rebalance because reached max rebalances count for widest range")
        else:
            self.rebalance_and_add_symmetric_liquidity(Decimal(range_for_rebalance))

            if range_for_rebalance == self.ranges[-1]:
                self.max_range_active_since = timestamp
            else:
                self.max_range_active_since = Timestamp(0)

            self.day_to_rebalance_count[key] = rebalanced_count + 1
            logging.info(
                f"({timestamp}) rebalance count for current day: {self.day_to_rebalance_count[key]}")

    def can_rebalance(self):
        return True

    def clear_state(self):
        self.max_range_active_since = Timestamp(0)
