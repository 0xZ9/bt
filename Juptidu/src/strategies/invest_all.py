from _decimal import Decimal

from strategies.brokkr_strategy import BrokkrStrategy
from utils.ui_utils import parse_positive_float


class InvestAll(BrokkrStrategy):
    day_to_rebalance_count = {}

    @staticmethod
    def name():
        return "Simple investment in range"

    @staticmethod
    def get_arguments():
        return [{
            "name": "Range (in percentage)",
            "example": "2",
            "description": "",
            "parser": parse_positive_float
        }]

    def __init__(self, range_in_percentage=5):
        super().__init__()
        self.range_in_percentage = range_in_percentage

    def initialize_custom(self):
        self.rebalance_and_add_symmetric_liquidity(Decimal(self.range_in_percentage))
