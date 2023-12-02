from _decimal import Decimal

from typing import List, Dict, Tuple, Optional

from demeter import MarketDict, RowData, UniLpMarket, Position
from demeter._typing import PositionInfo

from constants import ONE, PERCENT_TO_DECIMAL
from strategies.brokkr_strategy import BrokkrStrategy, RebalanceDirection
from utils.ui_utils import parse_positive_integer, parse_positive_decimal


class PriceThreshold:
    index: int                                  # index in division_price_points_{up | down}
    price: Decimal                              # price at which threshold is set
    rebalance_direction: RebalanceDirection     # direction in which threshold is placed compared to the init price

    def __init__(self, index: int, price: Decimal, rebalance_direction: RebalanceDirection):
        self.index = index
        self.price = price
        self.rebalance_direction = rebalance_direction

    def __eq__(self, other):
        return (other is not None
                and self.index == other.index
                and self.price == other.price
                and self.rebalance_direction == other.rebalance_direction)

    def __str__(self):
        return f"PriceThreshold: index={self.index}, price={self.price}, rebalance_direction={self.rebalance_direction}"

    def __repr__(self):
        return f"PriceThreshold: index={self.index}, price={self.price}, rebalance_direction={self.rebalance_direction}"


class PartialRebalanceData:
    threshold: PriceThreshold           # threshold at which rebalance happened
    liquidity_withdrawn: Decimal        # amount of liquidity withdrawn
    withdraw_percent_decimal: Decimal   # withdraw percentage in decimal

    def __init__(self, threshold: PriceThreshold, liquidity_withdrawn: Decimal, withdraw_percent_decimal: Decimal):
        self.threshold = threshold
        self.liquidity_withdrawn = liquidity_withdrawn
        self.withdraw_percent_decimal = withdraw_percent_decimal

    def __str__(self):
        return (f"PartialRebalanceData: threshold={self.threshold}, liquidity_withdrawn={self.liquidity_withdrawn},"
                f" withdraw_percent_decimal={self.withdraw_percent_decimal}")

    def __repr__(self):
        return (f"PartialRebalanceData: threshold={self.threshold}, liquidity_withdrawn={self.liquidity_withdrawn},"
                f" withdraw_percent_decimal={self.withdraw_percent_decimal}")


class PartialRebalancePosition:
    division_price_points_up: List[PriceThreshold]
    division_price_points_down: List[PriceThreshold]
    init_price: Decimal
    last_partial_rebalance: Optional[PartialRebalanceData]
    position: Position
    position_info: PositionInfo

    def __init__(self, division_price_points_up: List[PriceThreshold], division_price_points_down: List[PriceThreshold],
                 init_price: Decimal, position: Position, position_info: PositionInfo):
        self.division_price_points_up = division_price_points_up
        self.division_price_points_down = division_price_points_down
        self.init_price = init_price
        self.last_partial_rebalance = None
        self.position = position
        self.position_info = position_info

    def __str__(self):
        return (f"\nPartialRebalancePosition: \ndivision_price_points_up={self.division_price_points_up}, "
                f"\ndivision_price_points_down={self.division_price_points_down}, \ninit_price={self.init_price}, "
                f"\nlast_partial_rebalance={self.last_partial_rebalance}, \nposition_liquidity={self.position.liquidity}")


class BrokkrStrategyPartialRebalance(BrokkrStrategy):
    """
    A strategy that would try to prolong its own life by a half rebalancing.
    Instead of rebalancing the whole position, we would withdraw only X% of
    the position when it would reach the Rebalance threshold (which would be
    lower than the actual range) and we would open a new position.
    """

    range_in_percentage: Decimal            # Symmetrical range to be used around current price
    division: int                           # How many times a range should be rebalanced before it gets out of range
    division_threshold_percentage: Decimal  # Derived as: range_in_percentage / division
    active_partial_rebalance_positions_dict: Dict[PositionInfo, PartialRebalancePosition]

    @staticmethod
    def name():
        return "Strategy Partial Rebalance"

    @staticmethod
    def get_arguments():
        return [{
            "name": "Range (in percentage)",
            "example": "5",
            "description": "Symmetrical range to be used around current price",
            "parser": parse_positive_decimal
        }, {
            "name": "Division",
            "example": "2",
            "description": "How many times a range should be rebalanced before it gets out of range",
            "parser": parse_positive_integer
        }]

    def __init__(self, range_in_percentage: Decimal, division: int):
        super().__init__()
        self.range_in_percentage = range_in_percentage
        self.division = division
        self.division_threshold_percentage = range_in_percentage / self.division
        self.active_partial_rebalance_positions_dict = dict()

    def initialize_custom(self):
        # initialise strategy by first opening symmetrical range "Position 1"
        self.create_new_symmetric_position(self.get_current_default_quote_token_market_price())

    def on_bar_custom(self, row_data: MarketDict[RowData]):
        if not len(self.account_status):
            return

        self.process_active_positions(self.get_current_default_quote_token_market_price())

    def process_active_positions(self, current_price: Decimal):
        # iterate active positions and process each in order to determine if and for how many thresholds were passed

        # iterate each position and check if it reached threshold
        for position_info in list(self.active_partial_rebalance_positions_dict):
            partial_rebalance_position = self.active_partial_rebalance_positions_dict[position_info]

            price_threshold = self.get_surpassed_threshold(current_price,
                                                           partial_rebalance_position.division_price_points_up,
                                                           partial_rebalance_position.division_price_points_down)

            last_partial_rebalance = partial_rebalance_position.last_partial_rebalance

            # if price_threshold exists and surpassed price threshold does not equal last threshold at which rebalance happened
            if price_threshold is not None and (last_partial_rebalance is None or (
                    last_partial_rebalance is not None and last_partial_rebalance.threshold != price_threshold)):

                # if active position count equals divison, collapse liquidity instead of creating new one
                if len(self.active_partial_rebalance_positions_dict) == self.division:
                    return self.collapse_positions_into_single()
                else:
                    self.partial_rebalance_liquidity(partial_rebalance_position, price_threshold, position_info)
            else:
                return

    def partial_rebalance_liquidity(self, partial_rebalance_position: PartialRebalancePosition,
                                    price_threshold: PriceThreshold, position_info: PositionInfo):
        market = self.get_market()
        current_price = self.get_current_default_quote_token_market_price()

        position = partial_rebalance_position.position

        # withdraw liquidity
        withdraw_percent_decimal = self.calculate_withdraw_percent_decimal(price_threshold.index)

        liquidity_withdraw_amount = Decimal(position.liquidity) * withdraw_percent_decimal

        self.withdraw(market, position_info, int(liquidity_withdraw_amount),
                      withdraw_percent_decimal == ONE)

        # save info about threshold surpassing into current position being processed
        partial_rebalance_position.last_partial_rebalance = PartialRebalanceData(price_threshold,
                                                                                 liquidity_withdraw_amount,
                                                                                 withdraw_percent_decimal)

        # rebalance assets to current price
        self.symmetrical_rebalance(market, self.get_current_default_quote_token_market_price())

        # provide liquidity (create new position)
        new_position_info, _, _, _ = self.add_liquidity(market, self.range_in_percentage)

        # calculate division price points for the position
        division_price_points_up, division_price_points_down = self.calculate_division_price_points(
            current_price, self.division,
            self.division_threshold_percentage)

        # add new position to the active partial rebalance positions dict
        self.active_partial_rebalance_positions_dict[new_position_info] = PartialRebalancePosition(
            division_price_points_up,
            division_price_points_down,
            current_price,
            self.get_position(new_position_info),
            new_position_info)

    def collapse_positions_into_single(self):
        # remove all positions liquidity
        for position_info in list(self.active_partial_rebalance_positions_dict):
            self.withdraw(self.get_market(), position_info, -1, True)

        # create new position
        self.create_new_symmetric_position(self.get_current_default_quote_token_market_price())

    def create_new_symmetric_position(self, init_price: Decimal):
        # create new position
        position_info, _, _, _ = self.rebalance_and_add_symmetric_liquidity(self.range_in_percentage)

        # calculate division price points for the position
        division_price_points_up, division_price_points_down = self.calculate_division_price_points(init_price,
                                                                                                    self.division,
                                                                                                    self.division_threshold_percentage)

        # save partial rebalance position data to the map
        self.active_partial_rebalance_positions_dict[position_info] = PartialRebalancePosition(division_price_points_up,
                                                                                               division_price_points_down,
                                                                                               init_price,
                                                                                               self.get_position(
                                                                                                   position_info),
                                                                                               position_info)

    def withdraw(self, market: UniLpMarket, position_info: PositionInfo, liquidity_withdraw_amount: int,
                 full_withdrawal: bool):
        self.remove_liquidity(market, position_info, None if full_withdrawal else liquidity_withdraw_amount)

        if full_withdrawal:
            # remove position info from the dict if we've completely removed the position from the market
            del self.active_partial_rebalance_positions_dict[position_info]

    def calculate_withdraw_percent_decimal(self, threshold_index: int) -> Decimal:
        return (ONE + Decimal(threshold_index)) / Decimal(self.division)

    def get_surpassed_threshold(self, price: Decimal, division_price_points_up: List[PriceThreshold],
                                division_price_points_down: List[PriceThreshold]) -> Optional[PriceThreshold]:
        """
        Determine which way price moved and find out how many thresholds price has surpassed.
        :return: Optional[Tuple[threshold_index, RebalanceDirection]]
        """

        # price moved up, surpassing at least 1st threshold
        if price >= division_price_points_up[0].price:
            # find index of the last threshold surpassed by the price
            threshold_index = next(i for i, threshold in enumerate(division_price_points_up) if price >= threshold.price
                                   and (i == len(division_price_points_up) - 1 or price < division_price_points_up[
                i + 1].price))
            return division_price_points_up[threshold_index]

        # price moved down, surpassing at least 1st threshold
        elif price <= division_price_points_down[0].price:
            # find index of the last threshold surpassed by the price
            threshold_index = next(
                i for i, threshold in enumerate(division_price_points_down) if price <= threshold.price
                and (i == len(division_price_points_down) - 1 or price > division_price_points_down[i + 1].price))
            return division_price_points_down[threshold_index]
        else:
            return None

    def calculate_division_price_points(self, price: Decimal, divisions: int,
                                        division_threshold_percentage: Decimal) -> Tuple[
        List[PriceThreshold], List[PriceThreshold]]:
        """
        Given the starting price, calculate price points at which division thresholds are applied for up and down price movement.
        E.g. starting price = 100, 2 divisions, division_threshold_percentage = 5% -> division_price_points_up = [105, 110], division_price_points_down = [~95, ~90)
        :return: Tuple[List[PriceThreshold], List[PriceThreshold]]
        """
        division_price_points_up = []
        division_price_points_down = []

        for division in range(1, divisions + 1):
            division_price_points_up.append(PriceThreshold(
                division - 1,
                price * Decimal(ONE + division * (PERCENT_TO_DECIMAL * division_threshold_percentage)),
                RebalanceDirection.UP
            ))
            division_price_points_down.append(PriceThreshold(
                division - 1,
                price / Decimal(ONE + division * (PERCENT_TO_DECIMAL * division_threshold_percentage)),
                RebalanceDirection.DOWN
            ))

        return division_price_points_up, division_price_points_down
