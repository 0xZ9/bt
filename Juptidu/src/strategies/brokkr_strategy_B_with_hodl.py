import logging
from _decimal import Decimal
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional, List
from demeter import UniLpMarket
from demeter import RowData, MarketDict
from pandas import Timestamp

from strategies.brokkr_strategy_B import BrokkrStrategyB
from utils.ui_utils import parse_floats_array, parse_integers_array, parse_out_of_range_threshold
from strategies.brokkr_strategy import RebalanceDirection


@dataclass
class HodlModeConfiguration:
    # Get out of LPing
    max_rebalances_count: Optional[int]
    max_rebalances_window: Optional[timedelta]
    max_price_change_per_period_in_percentage: float
    price_movement_period_in_hours: int

    # Go back to LPing
    return_price_percentage: float
    return_duration: timedelta

    hodl_mode_range: float = 0.0


@dataclass
class PricePoint:
    timestamp: Timestamp
    price: float


@dataclass
class HodlModeStatus:
    rebalances_up: List[Timestamp] = field(default_factory=list)
    rebalances_down: List[Timestamp] = field(default_factory=list)
    prices_for_returning_window: List[PricePoint] = field(default_factory=list)

    def find_highest_price_movement(self):
        min_price = min(self.prices_for_returning_window, key=lambda x: x.price).price
        max_price = max(self.prices_for_returning_window, key=lambda x: x.price).price

        return ((max_price - min_price) / min_price) * 100


class BrokkrStrategyBWithHodl(BrokkrStrategyB):
    hodl_configuration: HodlModeConfiguration
    hodl_status: HodlModeStatus = HodlModeStatus()
    is_hodl_mode_active = False

    out_of_range_threshold_up: float
    out_of_range_threshold_down: float

    @staticmethod
    def name():
        return "Strategy B with HODL"

    @staticmethod
    def get_arguments():
        return [
            {
                "name": "Ranges",
                "example": "2,5",
                "description": "Comma separated ranges. When first rebalance level is reached,"
                               " next one is being used. Any count of ranges is allowed.",
                "parser": parse_floats_array
            }, {
                "name": "out of range threshold UP (in percentage).",
                "example": "50",
                "description": "Threshold must be a number > 0 and <= 100. "
                               "If the number is 100, the strategy will rebalance when out of range, "
                               "if the number is 50, the strategy will rebalance when the price is in the middle between "
                               "the position price and the range border.",
                "parser": parse_out_of_range_threshold
            }, {
                "name": "out of range down threshold DOWN (in percentage).",
                "example": "50",
                "description": "Threshold must be a number > 0 and <= 100. "
                               "If the number is 100, the strategy will rebalance when out of range, "
                               "if the number is 50, the strategy will rebalance when the price is in the middle between "
                               "the position price and the range border.",
                "parser": parse_out_of_range_threshold
            },
            {
                "name": "max rebalances per day",
                "example": "2,5 - passing ranges=1,3,6 and rebalances=1,2,3 means 1 rebalance with range 1%, "
                           "2 rebalances with 3% and 3 rebalances with 6% range until the end of the day.",
                "description": "Comma separated rebalances, count of rebalances must be the same as ranges, first rebalance"
                               " level refers to first first range, and so on.",
                "parser": parse_integers_array
            },
            {
                "name": "HODL mode configuration",
                "example": "2,6.5,1.5,8,2,3,0 or 1.5,8,2,3,500",
                "description": "Comma separated HODL configuration as follows: X,Y,Z,W,A,B,C or Z,W,A,B,C where Y and B (duration)"
                               " is always number of hours\n"
                               "go in HODL:\n"
                               "X, Y -> Min rebalances (X) in one direction during last Y hours to go into HODL\n"
                               "Z -> Min price move in % during the last W hours to go into HODL\n"
                               "W -> Period in hours since now where we check price movement for going into HODL \n"
                               "Go out of HODL:\n"
                               "A ->  Max price change in % during last B hours to go out of HODL\n"
                               "B -> Min amount of hours that must pass from going into HODL to go out of HODL\n"
                               "C -> HODL mode range in % - if 0 - funds are held without investing, "
                               "otherwise the range of this width is opened \n",

                "parser": BrokkrStrategyBWithHodl.parse_hodl_configuration
            }
        ]

    # noinspection PyDefaultArgument
    def __init__(self, ranges=[2, 5], out_of_range_threshold_up=100.0, out_of_range_threshold_down=100.0,
                 max_rebalances=[3, 2],
                 hodl_configuration: HodlModeConfiguration = HodlModeConfiguration(3, timedelta(hours=12), 1, 24,
                                                                                   12,
                                                                                   timedelta(hours=10), 0),
                 ):
        assert len(ranges) == len(max_rebalances)
        super().__init__(ranges, max_rebalances)
        self.hodl_configuration = hodl_configuration
        self.ranges = ranges
        self.max_rebalances = max_rebalances
        self.out_of_range_threshold_up = out_of_range_threshold_up
        self.out_of_range_threshold_down = out_of_range_threshold_down

    def on_bar_custom(self, row_data: MarketDict[RowData]):
        if not len(self.account_status) or not self.can_rebalance():
            return

        market: UniLpMarket = self.markets.default
        current_timestamp: Timestamp = market.market_status.timestamp
        current_price = float(market.market_status.price)

        if not self.is_hodl_mode_active:
            if self.should_get_out_of_lp(current_timestamp):
                price_change_in_period = self.get_max_price_change_in_past_period(current_timestamp, timedelta(
                    hours=self.hodl_configuration.price_movement_period_in_hours))
                logging.info(
                    f"({current_timestamp}) Conditions to get out of LPing met (price: {current_price} {self.get_base_token().name}). "
                    f"Price moved by more than {self.hodl_configuration.max_price_change_per_period_in_percentage}% "
                    f"({price_change_in_period:.2f}%) "
                    f"in the last {self.hodl_configuration.price_movement_period_in_hours} hours.")
                self.get_out_of_lp(market, current_price, current_timestamp)
            else:
                self.handle_current_timestamp()

            positions = list(self.markets.default.positions)
            if len(positions) > 0 and self.position_range_threshold_reached(positions[0], self.out_of_range_threshold_up,
                                                                            self.out_of_range_threshold_down):
                self.rebalance_and_add_symmetric_liquidity(Decimal(self.ranges[0]))

        else:
            # Keep saving prices until you have full array of for the specified period by
            prices_list = self.hodl_status.prices_for_returning_window
            current_price_point = PricePoint(current_timestamp, current_price)
            prices_list.append(current_price_point)

            min_time_passed_to_go_back_to_lping = (current_timestamp - self.hodl_configuration.return_duration) >= \
                                                  prices_list[0].timestamp

            if min_time_passed_to_go_back_to_lping:
                prices_list.pop(0)

                price_movement = self.hodl_status.find_highest_price_movement()

                if price_movement < self.hodl_configuration.return_price_percentage:
                    # Go back to LPing
                    logging.info(
                        f"{current_timestamp} Going back to lping! Max price movement during the last {self.duration_to_hours(self.hodl_configuration.return_duration)}h is {price_movement:.2f}%")
                    self.go_back_to_lping()

    def on_successful_position_opened(self, timestamp: Timestamp, range_min: float, range_max: float,
                                      rebalance_direction: Optional[RebalanceDirection]):
        super().on_successful_position_opened(timestamp, range_min=range_min, range_max=range_max,
                                              rebalance_direction=rebalance_direction)
        if rebalance_direction:
            if rebalance_direction.value == RebalanceDirection.UP.value:
                self.hodl_status.rebalances_up.append(timestamp)
            elif rebalance_direction.value == RebalanceDirection.DOWN.value:
                self.hodl_status.rebalances_down.append(timestamp)

    def should_get_out_of_lp(self, current_timestamp: Timestamp):
        return self.should_go_into_hodl_due_to_price_change(
            current_timestamp) and \
            self.should_go_into_hodl_due_to_max_rebalances(current_timestamp=current_timestamp)

    def should_go_into_hodl_due_to_price_change(self, current_timestamp: Timestamp) -> bool:
        price_change = self.get_max_price_change_in_past_period(current_timestamp, timedelta(
            hours=self.hodl_configuration.price_movement_period_in_hours))

        return price_change > self.hodl_configuration.max_price_change_per_period_in_percentage

    def get_out_of_lp(self, market: UniLpMarket, current_price: float, current_timestamp: Timestamp):
        if self.hodl_configuration.hodl_mode_range <= 0:
            log_msg = f"({current_timestamp}) Going into HODL mode! Removing all liquidity and rebalancing at price {current_price:.2f} {self.get_base_token().name}"
            market.remove_all_liquidity()
            self.symmetrical_rebalance(market, Decimal(current_price))
            self.current_range_low = 0
            self.current_range_high = 0
        else:
            log_msg = (f"({current_timestamp}) Going into HODL mode! Entering position on "
                       f"${self.hodl_configuration.hodl_mode_range}% range")

            self.rebalance_and_add_symmetric_liquidity(Decimal(self.hodl_configuration.hodl_mode_range))

        self.is_hodl_mode_active = True
        logging.info(log_msg)

    def should_go_into_hodl_due_to_max_rebalances(self, current_timestamp: Timestamp) -> bool:
        if self.hodl_configuration.max_rebalances_count is None or self.hodl_configuration.max_rebalances_window is None:
            return True

        for timestamps, rebalance_direction in [[self.hodl_status.rebalances_up, RebalanceDirection.UP],
                                                [self.hodl_status.rebalances_down, RebalanceDirection.DOWN]]:
            if self.max_rebalances_reached(current_timestamp, timestamps, self.hodl_configuration.max_rebalances_count,
                                           self.hodl_configuration.max_rebalances_window):
                logging.info(
                    f"({current_timestamp}) Reached max rebalances {self.hodl_configuration.max_rebalances_count} during last {self.duration_to_hours(self.hodl_configuration.max_rebalances_window)} h, direction: {str(rebalance_direction)}")
                return True

        return False

    def max_rebalances_reached(self, current_timestamp: Timestamp, timestamps: List[Timestamp],
                               max_rebalances_count: int,
                               max_rebalances_window: timedelta) -> bool:
        oldest_allowed_timestamp = current_timestamp - max_rebalances_window

        eligible_rebalances = [timestamp for timestamp in timestamps if timestamp >= oldest_allowed_timestamp]

        return len(eligible_rebalances) >= max_rebalances_count

    def go_back_to_lping(self):
        self.clear_state()
        self.hodl_status = HodlModeStatus()
        self.is_hodl_mode_active = False
        self.rebalance_and_add_symmetric_liquidity(Decimal(self.ranges[0]))

    def duration_to_hours(self, duration: timedelta):
        return duration.total_seconds() / 3600

    @staticmethod
    def parse_hodl_configuration(config: bytes) -> Optional[HodlModeConfiguration]:
        try:
            numbers = config.split(sep=",")

            if len(numbers) == 5:
                return HodlModeConfiguration(
                    max_rebalances_count=None,
                    max_rebalances_window=None,
                    max_price_change_per_period_in_percentage=float(numbers[0]),
                    price_movement_period_in_hours=int(numbers[1]),
                    return_price_percentage=float(numbers[2]),
                    return_duration=timedelta(hours=float(numbers[3])),
                    hodl_mode_range=float(numbers[4])
                )
            elif len(numbers) == 7:
                return HodlModeConfiguration(
                    max_rebalances_count=int(numbers[0]),
                    max_rebalances_window=timedelta(hours=float(numbers[1])),
                    max_price_change_per_period_in_percentage=float(numbers[2]),
                    price_movement_period_in_hours=int(numbers[3]),
                    return_price_percentage=float(numbers[4]),
                    return_duration=timedelta(hours=float(numbers[5])),
                    hodl_mode_range=float(numbers[6])
                )
            else:
                return None
        except:
            return None
