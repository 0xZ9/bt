import logging
from _decimal import Decimal
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from demeter import RowData, MarketDict
from pandas import Timestamp

from strategies.brokkr_strategy import BrokkrStrategy
from utils.ui_utils import parse_positive_float, parse_out_of_range_threshold


@dataclass
class IncreaseTheRangeConfig:
    max_price_change_hours: float  # B
    average_price_change_days: int  # C
    range: float  # D


@dataclass
class DecreaseTheRangeConfig:
    max_price_change_hours: float  # E
    average_price_change_days: int  # F


@dataclass
class ExtendAndDecreaseConfig:
    initial_range: float  # A
    out_of_range_threshold_in_percentage: float

    increase_the_range_config: IncreaseTheRangeConfig
    decrease_the_range_config: DecreaseTheRangeConfig


class BrokkrStrategyExtendAndDecrease(BrokkrStrategy):
    config: ExtendAndDecreaseConfig
    initialized = False
    is_range_increased = False

    @staticmethod
    def name():
        return "Extend and decrease strategy"

    @staticmethod
    def get_arguments():
        return [
            {
                "name": "Initial range (in percentage)",
                "example": "2",
                "description": "",
                "parser": parse_positive_float
            }, {
                "name": "out of range threshold (in percentage).",
                "example": "50",
                "description": "Threshold must be a number > 0 and <= 100. "
                               "If the number is 100, the strategy will rebalance when out of range, "
                               "if the number is 50, the strategy will rebalance when the price is in the middle between "
                               "the position price and the range border.",
                "parser": parse_out_of_range_threshold
            }, {
                "name": "widening the range configuration (B, C, D)",
                "example": "12,3,10",
                "description": "B - number of past hours to calculate max price change\n"
                               "C - number of past days to calculate average price change\n"
                               "D - Widened range in percentage\n",
                "parser": BrokkrStrategyExtendAndDecrease.parse_increase_the_range_config
            }, {
                "name": "narrowing the range configuration (E, F)",
                "example": "8,3",
                "description": "E - number of past hours to calculate max price change\n"
                               "F - number of past days to calculate average price change\n",
                "parser": BrokkrStrategyExtendAndDecrease.parse_decrease_the_range_config
            }

        ]

    def __init__(self, initial_range: float = 2,
                 out_of_range_threshold: float = 100,
                 increase_the_range_config: IncreaseTheRangeConfig = IncreaseTheRangeConfig(
                     max_price_change_hours=12,
                     average_price_change_days=3, range=10),
                 decrease_the_range_config: DecreaseTheRangeConfig = DecreaseTheRangeConfig(
                     max_price_change_hours=12,
                     average_price_change_days=3)):
        super().__init__()
        self.config = ExtendAndDecreaseConfig(
            initial_range=initial_range,
            out_of_range_threshold_in_percentage=out_of_range_threshold,
            increase_the_range_config=increase_the_range_config,
            decrease_the_range_config=decrease_the_range_config)

        assert (self.config.increase_the_range_config.max_price_change_hours <
                self.config.increase_the_range_config.average_price_change_days * 24)
        assert (self.config.decrease_the_range_config.max_price_change_hours <
                self.config.decrease_the_range_config.average_price_change_days * 24)

    def on_bar_custom(self, row_data: MarketDict[RowData]):
        if not len(self.account_status):
            return

        delay_time = timedelta(days=max(self.config.increase_the_range_config.average_price_change_days,
                                        self.config.decrease_the_range_config.average_price_change_days))
        timestamp = self.markets.default.market_status.timestamp
        start_timestamp = self.account_status[0].timestamp

        if self.initialized:
            positions = list(self.markets.default.positions)
            if len(positions) > 0 and self.is_position_out_of_range(positions[0],
                                                                    self.config.out_of_range_threshold_in_percentage):
                rebalance_range = self.config.increase_the_range_config.range if self.is_range_increased \
                    else self.config.initial_range
                self.rebalance_and_add_symmetric_liquidity(Decimal(rebalance_range))
            elif self.is_range_increased:
                self.on_range_increased(timestamp)
            else:
                self.on_normal_range(timestamp)
        elif timestamp >= start_timestamp + delay_time:
            self.initialize_extend_and_decrease_strategy()
            self.initialized = True

    def initialize_extend_and_decrease_strategy(self):
        self.rebalance_and_add_symmetric_liquidity(Decimal(self.config.initial_range))

    def on_range_increased(self, timestamp: Timestamp):
        maximum_price_change = self.get_max_price_change_in_past_period(timestamp,
                                                                        timedelta(
                                                                            hours=self.config.decrease_the_range_config.max_price_change_hours))
        average_price_change = self.get_average_max_daily_price_change(
            self.config.decrease_the_range_config.average_price_change_days,
            self.markets.default.market_status.timestamp)
        if maximum_price_change < average_price_change:
            logging.info(f"Maximum price change ({maximum_price_change:.4f}) in the last "
                         f"{self.config.decrease_the_range_config.max_price_change_hours}h < "
                         f"{average_price_change:.4f} average maximum daily price change in the last "
                         f"{self.config.decrease_the_range_config.average_price_change_days} days. "
                         f"Returning to the initial range of {self.config.initial_range}%")

            self.rebalance_and_add_symmetric_liquidity(Decimal(self.config.initial_range))
            self.is_range_increased = False
            logging.info(f"Range decreased to {self.config.initial_range}%")

    def on_normal_range(self, timestamp: Timestamp):
        maximum_price_change = self.get_max_price_change_in_past_period(timestamp, timedelta(
            hours=self.config.increase_the_range_config.max_price_change_hours))
        average_price_change = self.get_average_max_daily_price_change(
            self.config.increase_the_range_config.average_price_change_days, timestamp)

        if maximum_price_change > average_price_change:
            logging.info(f"Maximum price change ({maximum_price_change:.4f}) in the last "
                         f"{self.config.increase_the_range_config.max_price_change_hours}h > "
                         f"{average_price_change:.4f} average maximum daily price change in the last "
                         f"{self.config.increase_the_range_config.average_price_change_days} days. "
                         f"Increasing the range to {self.config.increase_the_range_config.range} %")
            self.increase_range()

    @staticmethod
    def parse_increase_the_range_config(user_input: bytes) -> Optional[IncreaseTheRangeConfig]:
        try:
            numbers = user_input.split(sep=",")

            if len(numbers) == 3:
                return IncreaseTheRangeConfig(
                    max_price_change_hours=float(numbers[0]),
                    average_price_change_days=int(numbers[1]),
                    range=float(numbers[2]))
        except:
            return None

    @staticmethod
    def parse_decrease_the_range_config(user_input: bytes) -> Optional[DecreaseTheRangeConfig]:
        try:
            numbers = user_input.split(sep=",")

            if len(numbers) == 2:
                return DecreaseTheRangeConfig(
                    max_price_change_hours=float(numbers[0]),
                    average_price_change_days=int(numbers[1]))
        except:
            return None

    def increase_range(self):
        self.rebalance_and_add_symmetric_liquidity(Decimal(self.config.increase_the_range_config.range))
        self.is_range_increased = True
