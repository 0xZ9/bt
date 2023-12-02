from demeter import RowData, MarketDict
from _decimal import Decimal
from strategies.brokkr_strategy import BrokkrStrategy
from utils.ui_utils import parse_positive_float, parse_out_of_range_threshold


class BrokkrStrategyAsymmetric(BrokkrStrategy):
    # asymmetric up and down ranges denominated in percentage (i.e. 5% = 5)
    range_dwn_in_percentage: Decimal
    range_up_in_percentage: Decimal
    out_of_range_threshold_up: float
    out_of_range_threshold_down: float
    last_active_position_start_price: Decimal

    @staticmethod
    def name():
        return "Asymmetric range strategy"

    @staticmethod
    def get_arguments():
        return [{
            "name": "Range down (in percentage)",
            "example": "2",
            "description": "",
            "parser": parse_positive_float
        }, {
            "name": "Range up (in percentage)",
            "example": "2",
            "description": "",
            "parser": parse_positive_float
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
        }
        ]

    def __init__(self, range_dwn_in_percentage: float, range_up_in_percentage: float, out_of_range_threshold_up: float,
                 out_of_range_threshold_down: float):
        super().__init__()
        self.range_dwn_in_percentage = Decimal(range_dwn_in_percentage)
        self.range_up_in_percentage = Decimal(range_up_in_percentage)
        self.out_of_range_threshold_up = out_of_range_threshold_up
        self.out_of_range_threshold_down = out_of_range_threshold_down

    def initialize_custom(self):
        self.rebalance_and_add_asymmetric_liquidity(self.range_dwn_in_percentage, self.range_up_in_percentage)
        self.last_active_position_start_price = self.get_current_default_quote_token_market_price()

    # This function act as a callback function triggered on a certain interval (2sec currently?)
    def on_bar_custom(self, row_data: MarketDict[RowData]):
        if not len(self.account_status):
            return

        positions = list(self.markets.default.positions)

        if len(positions) > 0 and self.position_range_threshold_reached(positions[0],
                                                                        self.out_of_range_threshold_up,
                                                                        self.out_of_range_threshold_down,
                                                                        self.last_active_position_start_price):
            self.rebalance_and_add_asymmetric_liquidity(self.range_dwn_in_percentage, self.range_up_in_percentage)
            self.last_active_position_start_price = self.get_current_default_quote_token_market_price()


