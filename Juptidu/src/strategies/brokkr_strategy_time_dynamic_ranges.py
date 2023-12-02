from _decimal import Decimal
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Optional

from demeter import MarketDict, RowData, UniLpMarket
from pandas import Timestamp

from strategies.brokkr_strategy import BrokkrStrategy
from utils.ui_utils import parse_positive_integer


class TimeDynamicRangesMode(Enum):
    Max = 1
    Average = 2


@dataclass
class HodlModeConfiguration:
    # Go to hodl
    max_price_change_per_period_in_percentage: float
    price_movement_period_in_hours: float

    # Exiting HODL
    return_price_percentage: float
    return_duration_in_hours: float


class BrokkrStrategyTimeDynamicRanges(BrokkrStrategy):
    mode: TimeDynamicRangesMode
    last_days_count: int
    hodl_mode_configuration: HodlModeConfiguration

    is_enough_data_to_start_flag = False
    hodl_mode_start_timestamp: Optional[Timestamp] = None

    @staticmethod
    def name():
        return "Brokkr time dynamic ranges"

    @staticmethod
    def get_arguments():
        return [{
            "name": "past price movement calculation mode",
            "example": "1",
            "description": "1. Max\n2. Average\n",
            "parser": BrokkrStrategyTimeDynamicRanges.parse_time_dynamic_ranges_mode
        }, {
            "name": "number of days to consider in past price movement calculation.",
            "example": "1",
            "description": "",
            "parser": parse_positive_integer
        },
            {
                "name": "Hodl mode configuration (Y,Z,A,B).",
                "example": "3,12,12,10",
                "description": "Y, Z - If the price moves more than Y% in Z hours, go to HODL\n"
                               "A, B - If the price has moved less than A% in B hours, exit HODL\n",
                "parser": BrokkrStrategyTimeDynamicRanges.parse_hodl_configuration
            }
        ]

    def __init__(self, mode=TimeDynamicRangesMode.Max, last_days_count=1,
                 hodl_configuration: HodlModeConfiguration = HodlModeConfiguration(3, 12,
                                                                                   12,
                                                                                   10)):
        super().__init__()
        self.mode = mode
        self.last_days_count = last_days_count
        self.hodl_mode_configuration = hodl_configuration

    def on_bar_custom(self, row_data: MarketDict[RowData]):
        if not self.is_enough_data_to_start():
            return

        market: UniLpMarket = self.markets.default
        timestamp: Timestamp = market.market_status.timestamp
        is_not_invested = len(market.positions) == 0

        if self.hodl_mode_active():
            if self.should_exit_hodl_mode(current_timestamp=timestamp):
                self.exit_hodl_mode(current_timestamp=timestamp)
            else:
                # Stay in hodl
                return
        elif self.should_go_to_hodl_mode(current_timestamp=timestamp):
            self.go_to_hodl_mode(current_timestamp=timestamp, market=market)
        elif is_not_invested or self.is_out_of_range():
            range = self.get_range_for_last_n_days(self.last_days_count, timestamp)
            self.rebalance_and_add_symmetric_liquidity(Decimal(range))

    def is_enough_data_to_start(self):
        if self.is_enough_data_to_start_flag:
            return True
        else:
            starting_timestamp = self.markets.default.data.iloc[0].name
            current_timestamp = self.markets.default.market_status.timestamp
            difference = current_timestamp - starting_timestamp

            self.is_enough_data_to_start_flag = difference >= timedelta(days=self.last_days_count)
            return self.is_enough_data_to_start_flag

    def get_range_for_last_n_days(self, last_days_count: int, current_timestamp: Timestamp) -> float:
        if self.mode.value == TimeDynamicRangesMode.Max.value:
            return float(self.get_max_price_change_in_past_period(current_timestamp, timedelta(days=last_days_count)))
        elif self.mode.value == TimeDynamicRangesMode.Average.value:
            return self.get_average_max_daily_price_change(last_days_count, current_timestamp)
        else:
            raise Exception("Unsupported mode")

    def hodl_mode_active(self):
        return self.hodl_mode_start_timestamp is not None

    def should_exit_hodl_mode(self, current_timestamp: Timestamp):
        min_time_passed = (current_timestamp - self.hodl_mode_start_timestamp) >= timedelta(
            hours=self.hodl_mode_configuration.return_duration_in_hours)

        if min_time_passed:
            price_change_in_percentage = float(self.get_max_price_change_in_past_period(current_timestamp, timedelta(
                hours=self.hodl_mode_configuration.return_duration_in_hours)))

            if price_change_in_percentage <= self.hodl_mode_configuration.return_price_percentage:
                print(
                    f"({current_timestamp}) Price change during last {self.hodl_mode_configuration.return_duration_in_hours}h "
                    f"{price_change_in_percentage:.2f}% is lower than "
                    f"{self.hodl_mode_configuration.return_price_percentage:.2f}%. Exiting HODL.")
                return True
            else:
                print(
                    f"({current_timestamp}) Price change during last {self.hodl_mode_configuration.return_duration_in_hours}h "
                    f"{price_change_in_percentage:.2f}% is higher than "
                    f"{self.hodl_mode_configuration.return_price_percentage:.2f}%. Staying in HODL...")
                return False

        else:
            return False

    def exit_hodl_mode(self, current_timestamp):
        self.hodl_mode_start_timestamp = None
        range = self.get_range_for_last_n_days(self.last_days_count, current_timestamp)
        self.rebalance_and_add_symmetric_liquidity(Decimal(range))

    def should_go_to_hodl_mode(self, current_timestamp: Timestamp) -> bool:
        price_change = float(self.get_max_price_change_in_past_period(current_timestamp, timedelta(
            hours=self.hodl_mode_configuration.price_movement_period_in_hours)))

        if price_change > self.hodl_mode_configuration.max_price_change_per_period_in_percentage:
            print(
                f'({current_timestamp}) Price change in last {self.hodl_mode_configuration.price_movement_period_in_hours}h: '
                f'{price_change:.2f}% > {self.hodl_mode_configuration.max_price_change_per_period_in_percentage:.2f}%. '
                f'Going into HODL.')
            return True
        else:
            return False

    def go_to_hodl_mode(self, current_timestamp: Timestamp, market: UniLpMarket):
        self.hodl_mode_start_timestamp = current_timestamp
        market.remove_all_liquidity()
        self.current_range_low = 0
        self.current_range_high = 0

    @staticmethod
    def parse_hodl_configuration(config: bytes) -> Optional[HodlModeConfiguration]:
        try:
            numbers = config.split(sep=",")

            if len(numbers) == 4:
                return HodlModeConfiguration(
                    max_price_change_per_period_in_percentage=float(numbers[0]),
                    price_movement_period_in_hours=float(numbers[1]),
                    return_price_percentage=float(numbers[2]),
                    return_duration_in_hours=float(numbers[3])
                )
        except:
            return None

    @staticmethod
    def parse_time_dynamic_ranges_mode(input: bytes) -> Optional[TimeDynamicRangesMode]:
        if input.isdigit() and (int(input) == 1 or int(input) == 2):
            return TimeDynamicRangesMode(int(input))
