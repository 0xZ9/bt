import logging
from _decimal import Decimal
from collections import namedtuple
from datetime import timedelta
from enum import Enum
from sqlite3 import Timestamp
from typing import Optional, List

from demeter import MarketDict, RowData, UniLpMarket

from strategies.brokkr_strategy import BrokkrStrategy
from utils.ui_utils import parse_positive_float, parse_positive_integer


class OutOfRangeDirection(Enum):
    UP = 1
    DOWN = 2


Block = namedtuple("Block", ["l", "h"])


class BrokkrStrategyD(BrokkrStrategy):
    range: float
    amount_of_blocks: int
    max_rebalances_per_24h: int
    rebalances: List[Timestamp] = []

    def __init__(self, range=2, amount_of_blocks=3, max_rebalances_per_24h=3):
        super().__init__()

        self.range = range
        self.amount_of_blocks = amount_of_blocks
        self.max_rebalances_per_24h = max_rebalances_per_24h

    @staticmethod
    def name():
        return "Strategy D - block ranges"

    @staticmethod
    def get_arguments():
        return [{
            "name": "Range",
            "example": "2.5",
            "description": "Range that will be applied to blocks",
            "parser": parse_positive_float
        }, {
            "name": "Amount of blocks",
            "example": "3",
            "description": "Number of additional blocks on each side of the active block. E.g. when passing 3, total "
                           "number of blocks is 7 - 1 active block, 3 on the left and 3 on the right (n*2+1)",
            "parser": parse_positive_integer
        }, {
            "name": "Max rebalances per 24h",
            "example": "3",
            "description": "",
            "parser": parse_positive_integer
        }]

    def initialize_custom(self):
        base_token = self.get_base_token()
        quote_token = self.get_quote_token()
        market: UniLpMarket = self.markets.default
        current_price = market.market_status.price
        timestamp = market.market_status.timestamp

        self.symmetrical_rebalance(market, current_price)

        number_of_positions = (self.amount_of_blocks * 2) + 1
        assets_per_position = Decimal(1 / number_of_positions)

        base_holdings, quote_holdings = self.get_base_and_quote_holdings()
        range_decimal = Decimal(self.range * 0.01)

        active_block, positions_to_open = self.get_all_positions_to_open(current_price, self.amount_of_blocks,
                                                                         range_decimal)

        self.add_liquidity_custom_range(market, active_block[0],
                                        active_block[1],
                                        base_max_amount=Decimal(base_holdings * assets_per_position),
                                        quote_max_amount=Decimal(quote_holdings * assets_per_position))
        self.current_range_low = active_block[0]
        self.current_range_high = active_block[1]

        position_count = -self.amount_of_blocks
        for l, h in positions_to_open:
            self.add_liquidity_custom_range(market, l, h,
                                            base_max_amount=Decimal(base_holdings * assets_per_position) * 2,
                                            quote_max_amount=Decimal(quote_holdings * assets_per_position) * 2)

            logging.info(f"({timestamp}) Opened position  for the block number {position_count})")
            position_count += 1
            if position_count == 0:
                position_count += 1

        logging.info("Assets after initialization")
        self.print_assets()

    def get_all_positions_to_open(self, init_price: Decimal, amount_of_blocks: int, range_decimal: Decimal) -> [
        Block, [Block]]:
        blocks_range = list(filter(lambda x: x != 0, range(-amount_of_blocks, amount_of_blocks + 1)))
        all_blocks = []

        active_block = [init_price / Decimal((1 + range_decimal)), init_price * Decimal(1 + range_decimal)]
        last_low = active_block[0]
        last_high = active_block[1]

        blocks_range.sort(key=lambda b: abs(b))

        for block_count in blocks_range:
            if block_count > 0:
                l = last_high
                h = self.find_range_upper_border(l, range_decimal)

                all_blocks.append([l, h])
                last_high = h
            else:
                h = last_low
                l = self.find_range_low_border(h, range_decimal)

                all_blocks.append([l, h])
                last_low = l

        all_blocks.sort(key=lambda b: b[0])

        return [active_block, all_blocks]

    def on_bar_custom(self, row_data: MarketDict[RowData]):
        if not len(self.account_status):
            return

        base_token = self.get_base_token()
        quote_token = self.get_quote_token()
        range_decimal = Decimal(self.range * 0.01)
        market = self.markets.default
        current_price = market.market_status.price
        timestamp = market.market_status.timestamp

        middle_position = self.get_middle_position()
        out_of_range_block_direction = self.is_out_of_active_block(middle_position, current_price)

        if out_of_range_block_direction:
            if not self.can_rebalance(timestamp):
                logging.info(
                    f"({timestamp}) Skipping rebalance - reached max ({self.max_rebalances_per_24h}) 24h rebalances.")
                return

            position_to_close = self.get_position_to_close(out_of_range_block_direction)
            logging.info(f"({timestamp}) Got out of range, direction: {out_of_range_block_direction}. ")
            self.remove_liquidity(market, position_to_close)
            base_token_holdings, quote_token_holdings = self.get_base_and_quote_holdings()

            if out_of_range_block_direction == OutOfRangeDirection.UP:  # out of range direction -> UP
                quote_to_buy = (base_token_holdings / current_price) * (1 - market._pool.fee_rate)
                fee, quote_received, base_spent = market.buy(quote_to_buy)

                logging.info(
                    f"({timestamp}) Bought {quote_received:.5f} {quote_token.name} "
                    f"sold {base_spent:.2f} {base_token.name} fee {fee:.2f} {base_token.name}")

                lower_range = self.get_low_high_borders(self.get_highest_position())[1]  # High of the highest position
                upper_range = self.find_range_upper_border(lower_range, range_decimal)
                self.add_liquidity_custom_range(market, lower_range, upper_range)

                logging.info(
                    f"({timestamp}) Opened new position for the block number {self.amount_of_blocks}")
            else:  # out of range direction -> DOWN
                fee, quote_token_spent, base_received = market.sell(quote_token_holdings)
                logging.info(
                    f"({timestamp}) Sold {quote_token_spent:.5f} {quote_token.name}, got "
                    f"{base_received:.2f} {base_token.name} fee {fee:.10f} {quote_token.name}")

                upper_range = self.get_low_high_borders(self.get_lowest_position())[0]  # High of the highest position
                lower_range = self.find_range_low_border(upper_range, range_decimal)

                self.add_liquidity_custom_range(
                    market,
                    lower_range,
                    upper_range)

                logging.info(
                    f"({timestamp}) Opened new position for the block number {self.amount_of_blocks} ")

            self.on_successful_position_opened(timestamp)

    def get_middle_position(self):
        market = self.markets.default
        positions = list(market.positions)

        middleIndex = int((len(positions) - 1) / 2)
        positions.sort(key=lambda p: market.tick_to_price(p.lower_tick))

        return positions[middleIndex]

    def get_highest_position(self):
        market = self.markets.default
        positions = list(market.positions)

        positions.sort(key=lambda p: market.tick_to_price(p.lower_tick), reverse=True)

        return positions[0]

    def get_lowest_position(self):
        market = self.markets.default
        positions = list(market.positions)

        positions.sort(key=lambda p: market.tick_to_price(p.lower_tick))

        return positions[0]

    def can_rebalance(self, current_timestamp: Timestamp) -> bool:
        day_ago = current_timestamp - timedelta(days=1)
        rebalances_within_24h_count = len([timestamp for timestamp in self.rebalances if timestamp > day_ago])

        return rebalances_within_24h_count < self.max_rebalances_per_24h

    def on_successful_position_opened(self, timestamp: Timestamp, **kwargs):
        self.rebalances.append(timestamp)
        middle_position = self.get_low_high_borders(self.get_middle_position())
        self.current_range_low = middle_position[0]
        self.current_range_high = middle_position[1]

    def get_position_to_close(self, outOfRangeDirection: OutOfRangeDirection):
        market = self.markets.default
        positions = list(market.positions)

        positions.sort(key=lambda p: market.tick_to_price(p.lower_tick))

        if outOfRangeDirection == OutOfRangeDirection.DOWN:
            return positions[-1]
        else:
            return positions[0]

    def is_out_of_active_block(self, position, price: Decimal) -> Optional[OutOfRangeDirection]:
        l, h = self.get_low_high_borders(position)

        if price > h:
            return OutOfRangeDirection.UP
        elif price < l:
            return OutOfRangeDirection.DOWN
        else:
            return None

    def find_range_low_border(self, upper_range_price: Decimal, range_decimal: Decimal):
        range_price = upper_range_price / Decimal(1 + range_decimal)

        return range_price / Decimal((1 + range_decimal))

    def find_range_upper_border(self, lower_range_price: Decimal, range_decimal: Decimal):
        range_price = lower_range_price * Decimal((1 + range_decimal))

        return range_price * Decimal(1 + range_decimal)
