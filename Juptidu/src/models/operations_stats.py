from _decimal import Decimal
from dataclasses import dataclass


@dataclass
class OperationsStats:
    rebalances_count = 0
    providing_lp_count = 0
    withdrawing_lp_count = 0

    rebalances_cost = Decimal(0)
    providing_lp_cost = Decimal(0)
    withdrawing_lp_cost = Decimal(0)
