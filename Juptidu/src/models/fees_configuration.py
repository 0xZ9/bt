from dataclasses import dataclass

from demeter import TokenInfo

from config.fees import FeeMap


@dataclass
class FeesConfiguration:
    token: TokenInfo
    fee_rates: FeeMap