from datetime import timedelta
from typing import Optional

from pandas import Timestamp

from strategies.brokkr_strategy_B import BrokkrStrategyB
from utils.ui_utils import parse_floats_array, parse_integers_array, parse_positive_integer
from strategies.brokkr_strategy import RebalanceDirection


class BrokkrStrategyC(BrokkrStrategyB):
    last_successful_rebalance = Timestamp(0)

    @staticmethod
    def name():
        return "Strategy C"

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
        }, {
            "name": "Min time that must pass between rebalances (in minutes)",
            "example": "60",
            "description": "",
            "parser": parse_positive_integer
        }]

    # noinspection PyDefaultArgument
    def __init__(self, ranges=[2, 5], max_rebalances=[3, 2], min_time_between_rebalances_in_minutes=60):
        assert len(ranges) == len(max_rebalances)
        super().__init__()
        self.ranges = ranges
        self.max_rebalances = max_rebalances
        self.min_time_between_rebalances_in_minutes = min_time_between_rebalances_in_minutes

    def on_successful_position_opened(self, timestamp: Timestamp, range_min: float, range_max: float,
                                      rebalance_direction: Optional[RebalanceDirection]):
        super().on_successful_position_opened(timestamp, range_min=range_min, range_max=range_max,
                                              rebalance_direction=rebalance_direction)
        self.last_successful_rebalance = timestamp

    def can_rebalance(self) -> bool:
        current_timestamp: Timestamp = self.account_status[-1].timestamp

        can_rebalance = self.is_older_than(self.last_successful_rebalance, current_timestamp,
                                           self.min_time_between_rebalances_in_minutes)

        return can_rebalance

    def is_older_than(self, historical_timestamp, current_timestamp, minutes):
        duration = timedelta(minutes=minutes)
        time_difference = current_timestamp - historical_timestamp
        return time_difference >= duration
