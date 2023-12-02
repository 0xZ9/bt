from _decimal import Decimal
from dataclasses import dataclass
from typing import Any

from demeter import ChainType


@dataclass
class Fee:
    swap_in: Decimal
    liquidity_providing: Decimal
    removing_liquidity: Decimal


FeeMap = dict[int, Fee]
CoinNames = list[str]
# Source: https://docs.google.com/spreadsheets/d/1BUseJr4aLcHMmDOyJlctAA8b2yYpqYK9TMdVDgGfb9U/edit#gid=1027636602
ethereum_fees_native: FeeMap = {
    2015: Fee(Decimal("0.00627367"), Decimal("0.01886801"), Decimal("0.01072989")),
    2016: Fee(Decimal("0.00469774"), Decimal("0.01412843"), Decimal("0.00803458")),
    2017: Fee(Decimal("0.00336409"), Decimal("0.01011747"), Decimal("0.00575361")),
    2018: Fee(Decimal("0.00285333"), Decimal("0.00858136"), Decimal("0.00488006")),
    2019: Fee(Decimal("0.00223397"), Decimal("0.00671864"), Decimal("0.00382077")),
    2020: Fee(Decimal("0.00835881"), Decimal("0.02513907"), Decimal("0.01429612")),
    2021: Fee(Decimal("0.01450546"), Decimal("0.04362507"), Decimal("0.02480876")),
    2022: Fee(Decimal("0.00670571"), Decimal("0.02016738"), Decimal("0.01146881")),
    2023: Fee(Decimal("0.00512012"), Decimal("0.01539873"), Decimal("0.00875697"))
}
ethereum_fees_usdc: FeeMap = {
    2015: Fee(Decimal("11.48"), Decimal("34.53"), Decimal("19.64")),
    2016: Fee(Decimal("8.60"), Decimal("25.86"), Decimal("14.70")),
    2017: Fee(Decimal("6.16"), Decimal("18.51"), Decimal("10.53")),
    2018: Fee(Decimal("5.22"), Decimal("15.70"), Decimal("8.93")),
    2019: Fee(Decimal("4.09"), Decimal("12.30"), Decimal("6.99")),
    2020: Fee(Decimal("15.30"), Decimal("46.00"), Decimal("26.16")),
    2021: Fee(Decimal("26.54"), Decimal("79.83"), Decimal("45.40")),
    2022: Fee(Decimal("12.27"), Decimal("36.91"), Decimal("20.99")),
    2023: Fee(Decimal("9.37"), Decimal("28.18"), Decimal("16.03"))
}

arbitrum_fees_native: FeeMap = {
    2015: Fee(Decimal("0.00007979"), Decimal("0.00013726"), Decimal("0.00011422"))
}

arbitrum_fees_usdc: FeeMap = {
    2015: Fee(Decimal("0.15"), Decimal("0.25"), Decimal("0.21"))
}

chain_to_fees_dict: dict[ChainType, Any] = {
    # Native tokens should be first as they are preferred option and first match is always used.
    # (e.g. for the pool ETH/USDC the fees in ETH will be used only if it's the first match, before USDC)
    ChainType.Ethereum: [
        [["eth", "weth"], ethereum_fees_native],
        [["usd", "usdc"], ethereum_fees_usdc]
    ],
    ChainType.Arbitrum: [
        [["arb"], arbitrum_fees_native],
        [["usd", "usdc"], arbitrum_fees_usdc],
    ]
}

native_token_map: dict[ChainType, [str]] = {
    ChainType.Ethereum: ["eth", "weth"],
    ChainType.Arbitrum: ["arb"]
}
