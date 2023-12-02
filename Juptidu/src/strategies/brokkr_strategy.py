import logging
import statistics
from _decimal import Decimal
from abc import abstractmethod
from datetime import timedelta
from typing import Optional, Dict, List, Tuple
from demeter import Strategy, UniLpMarket, MarketDict, RowData, TokenInfo, ChainType, Position
from demeter._typing import PositionInfo
from pandas import Series
from pandas import Timestamp

from constants import ONE

from config.fees import chain_to_fees_dict, Fee
from models.fees_configuration import FeesConfiguration
from models.operations_stats import OperationsStats
from models.rebalance_direction import RebalanceDirection
from utils.strategy_utils import market_asymmetric_rebalance, get_price_change_in_percentage, \
    get_lower_upper_price_for_symmetric_range, get_lower_upper_price_for_asymmetric_range, \
    calculate_base_diff_for_asymmetric_position, get_rebalance_direction


class BrokkrStrategy(Strategy):
    stats = OperationsStats()

    current_range_low = 0
    current_range_high = 0
    range_low_line = []
    range_high_line = []
    range_low_line_list: List[List[Tuple[PositionInfo, Decimal, Timestamp]]] = []
    range_high_line_list: List[List[Tuple[PositionInfo, Decimal, Timestamp]]] = []
    rebalances_line = []
    providing_lp_line = []
    removing_lp_line = []
    initial_investment_value_line = []
    initial_base_holdings = Decimal(0)
    initial_quote_holdings = Decimal(0)
    fees_configuration: FeesConfiguration
    chain: ChainType

    @staticmethod
    @abstractmethod
    def name():
        pass

    @staticmethod
    @abstractmethod
    def get_arguments():
        pass

    @abstractmethod
    def on_bar_custom(self, row_data: MarketDict[RowData]):
        pass

    @abstractmethod
    def initialize_custom(self):
        pass

    def on_bar(self, row_data: MarketDict[RowData]):
        self.on_bar_custom(row_data)
        self.range_low_line.append(self.current_range_low)
        self.range_high_line.append(self.current_range_high)
        self.rebalances_line.append(self.stats.rebalances_count)
        self.providing_lp_line.append(self.stats.providing_lp_count)
        self.removing_lp_line.append(self.stats.withdrawing_lp_count)
        self.snapshot_all_active_positions_ranges()

        price = self.markets.default.market_status.price
        self.initial_investment_value_line.append(
            self.initial_base_holdings + self.initial_quote_holdings * price)

    def initialize(self):
        if not hasattr(self, "chain"):
            raise Exception("Please set the chain before using a strategy!")

        market = self.markets.default
        self.fees_configuration = self.get_fees_configuration()

        if self.fees_configuration:
            logging.info("Loaded fee configuration:")
            logging.info(self.fees_configuration)
        else:
            logging.warning("No fee configuration found for this pool! Using 0 fees for all transactions.")

        self.initial_base_holdings = Decimal(1 - market._pool.fee_rate) * self.broker.assets[
            self.get_base_token()].balance / Decimal(2)
        price = self.markets.default.market_status.price
        self.initial_quote_holdings = self.initial_base_holdings / price

        self.initialize_custom()

    def finalize(self):
        self.data.default["range_low"] = self.range_low_line
        self.data.default["range_high"] = self.range_high_line
        self.data.default["rebalances"] = self.rebalances_line
        self.data.default["lp_providing"] = self.providing_lp_line
        self.data.default["lp_withdrawing"] = self.removing_lp_line
        self.data.default["initial_investment_value"] = self.initial_investment_value_line
        self.data.default["base_fees"] = self.get_fees_line('base_uncollected')
        self.data.default["quote_fees"] = self.get_fees_line('quote_uncollected')

        logging.info(f"Total rebalances count: {self.stats.rebalances_count}")

        if self.fees_configuration:
            self.print_cost_of_gas_fees(self.fees_configuration)

    def snapshot_all_active_positions_ranges(self):
        market = self.get_market()
        ranges_low = []
        ranges_high = []

        for position_info, position in self.get_positions().items():
            ranges_low.append((position_info, market.tick_to_price(position_info.lower_tick),
                               Timestamp(market.market_status.timestamp)))
            ranges_high.append((position_info, market.tick_to_price(position_info.upper_tick),
                                Timestamp(market.market_status.timestamp)))

        self.range_low_line_list.append(ranges_low)
        self.range_high_line_list.append(ranges_high)

    def print_cost_of_gas_fees(self, fee_configuration: FeesConfiguration):
        logging.info(f"Total rebalances cost: {self.stats.rebalances_cost} {fee_configuration.token.name}")
        logging.info(f"Total LP providing count: {self.stats.providing_lp_count}")
        logging.info(f"Total LP providing cost: {self.stats.providing_lp_cost} {fee_configuration.token.name}")
        logging.info(f"Total LP removing count: {self.stats.withdrawing_lp_count}")
        logging.info(f"Total LP removing cost: {self.stats.withdrawing_lp_cost} {fee_configuration.token.name}")

    def on_successful_position_opened(self, timestamp: Timestamp, range_min: float, range_max: float,
                                      rebalance_direction: Optional[RebalanceDirection]):
        self.current_range_low = range_min
        self.current_range_high = range_max

    def is_out_of_range(self):
        current_account_status = self.account_status[-1]
        base_balance = current_account_status.market_status.default.base_in_position
        quote_balance = current_account_status.market_status.default.quote_in_position

        return quote_balance <= 0 or base_balance <= 0

    def is_position_out_of_range(self, position: PositionInfo, out_of_range_percentage_threshold: float = 100):
        lower_price, upper_price = self.get_low_high_borders(position)
        current_price = self.markets.default.market_status.price
        position_price = (lower_price + upper_price) / 2

        if out_of_range_percentage_threshold < 0 or out_of_range_percentage_threshold > 100:
            raise Exception("out_of_range_percentage_threshold must be between 0 and 100")

        if position_price == current_price:
            return False
        elif current_price < lower_price or current_price > upper_price:
            return True

        price_on_the_left_side_of_position = current_price < position_price

        range_start = lower_price if price_on_the_left_side_of_position else position_price
        range_end = position_price if price_on_the_left_side_of_position else upper_price

        out_of_range_level = (current_price - range_start) / (range_end - range_start) * 100

        if price_on_the_left_side_of_position:
            out_of_range_level = 100 - out_of_range_level

        return out_of_range_level > out_of_range_percentage_threshold

    def position_range_threshold_reached(self, position: PositionInfo, out_of_range_threshold_up: float,
                                         out_of_range_threshold_down: float, position_start_price=None):
        lower_price, upper_price = self.get_low_high_borders(position)
        lower_price = float(lower_price)
        upper_price = float(upper_price)
        current_price = float(self.get_current_default_quote_token_market_price())
        position_mid_price = (lower_price + upper_price) / 2 if position_start_price is None else float(
            position_start_price)

        if 100 > out_of_range_threshold_up < 0 or 100 > out_of_range_threshold_down < 0:
            raise Exception("out of range threshold must be between 0 and 100")

        if position_mid_price == current_price:
            return False
        elif current_price < lower_price or current_price > upper_price:
            return True

        if current_price > position_mid_price:
            # price moved up from the mid (start) position price
            price_displacement_percent_up = ((current_price - position_mid_price) / (
                    upper_price - position_mid_price)) * 100
            return price_displacement_percent_up >= out_of_range_threshold_up
        else:
            # price moved down from the mid (start) position price
            price_displacement_percent_down = ((position_mid_price - current_price) / (
                    position_mid_price - lower_price)) * 100
            return price_displacement_percent_down >= out_of_range_threshold_down

    def rebalance_and_add_symmetric_liquidity(self, range_in_percentage: Decimal, init_price=None) -> (
            PositionInfo, Decimal, Decimal, int):
        lp_market: UniLpMarket = self.markets.default
        timestamp = lp_market.market_status.timestamp
        init_price = lp_market.market_status.price if init_price is None else init_price
        rebalance_direction: Optional[RebalanceDirection] = None

        if len(lp_market.positions):
            logging.debug(f"({timestamp}) Removing all liquidity from {len(lp_market.positions)} positions.")
            rebalance_direction = get_rebalance_direction(list(lp_market.positions).pop(), lp_market, init_price)
            self.remove_all_liquidity(lp_market)

        self.symmetrical_rebalance(lp_market, init_price)
        return self.add_liquidity(lp_market, range_in_percentage, rebalance_direction=rebalance_direction)

    def rebalance_and_add_asymmetric_liquidity(self, range_dwn_in_percentage: Decimal, range_up_in_percentage: Decimal):
        lp_market: UniLpMarket = self.markets.default
        timestamp = lp_market.market_status.timestamp
        base_token_price: Decimal = self.get_current_default_base_token_market_price()

        if len(lp_market.positions):
            logging.debug(f"({timestamp}) Removing all liquidity from {len(lp_market.positions)} positions.")
            self.remove_all_liquidity(lp_market)

        lower_quote_price, upper_quote_price = get_lower_upper_price_for_asymmetric_range(range_dwn_in_percentage,
                                                                                          range_up_in_percentage,
                                                                                          lp_market.market_status.price)

        base_holdings, quote_holdings = self.get_base_and_quote_holdings()
        total_capital_in_base = base_holdings + (quote_holdings / base_token_price)

        base_token_amount_diff = calculate_base_diff_for_asymmetric_position(base_token_price,
                                                                             base_holdings,
                                                                             total_capital_in_base,
                                                                             ONE / upper_quote_price,
                                                                             ONE / lower_quote_price,
                                                                             )

        if base_token_amount_diff < 0:
            # BUY QUOTE TOKEN BY SELLING BASE TOKEN
            # amount to buy (in quote token)
            self.get_market().buy(abs(base_token_amount_diff) * base_token_price)

        elif base_token_amount_diff > 0:
            # BUY BASE TOKEN BY GIVING QUOTE TOKEN AMOUNT IN
            # amount to sell(in quote token)
            self.get_market().sell(base_token_amount_diff * base_token_price)

        self.add_liquidity_custom_range(lp_market, lower_quote_price, upper_quote_price)

    def print_assets(self):
        str = "Broker's assets:\n"
        for item in self.broker.assets.items():
            str += f"{item[1]} "

        logging.info(str)

    def get_fees_line(self, property_name: str):
        fee_earned_from_previous_positions = Decimal(0)
        previous_fee_uncollected = Decimal(0)
        fees_line = []
        for account_status in self.account_status:
            fee_uncollected = getattr(account_status.market_status.default, property_name)

            if fee_uncollected < previous_fee_uncollected:
                fee_earned_from_previous_positions += previous_fee_uncollected

            fees_line.append(fee_earned_from_previous_positions + fee_uncollected)
            previous_fee_uncollected = fee_uncollected

        return fees_line

    def get_base_token(self) -> TokenInfo:
        return self.markets.default.base_token

    def get_quote_token(self) -> TokenInfo:
        return self.markets.default.quote_token

    def get_max_price_change_in_past_period(self, now: Timestamp, period: timedelta):
        from_timestamp = now - period
        price_series: Series = self.data.default.price
        prices_in_range = price_series[from_timestamp:now]
        highest_price = max(prices_in_range)
        lowest_price = min(prices_in_range)

        return abs(get_price_change_in_percentage(highest_price, lowest_price))

    def get_low_high_borders(self, position):
        market = self.markets.default

        l = market.tick_to_price(position.lower_tick)
        h = market.tick_to_price(position.upper_tick)

        return l, h

    def get_base_and_quote_holdings(self):
        base_token = self.get_base_token()
        quote_token = self.get_quote_token()

        base_holdings = self.broker.assets[base_token].balance
        quote_holdings = self.broker.assets[quote_token].balance

        return base_holdings, quote_holdings

    def position_to_string(self, position):
        l, h = self.get_low_high_borders(position)

        return f"{l:.2f}-{h:.2f}"

    def get_fees_configuration(self) -> Optional[FeesConfiguration]:
        tokens_in_pool = [self.get_base_token().name.lower(), self.get_quote_token().name.lower()]

        fees_configuration_for_current_chain = chain_to_fees_dict[self.chain]

        for tokens, fee_rates in fees_configuration_for_current_chain:
            match = set(tokens).intersection(tokens_in_pool)
            if len(match) > 0:
                matched_token_name = match.pop()

                token: TokenInfo

                if self.get_base_token().name.lower() == matched_token_name:
                    token = self.get_base_token()
                else:
                    token = self.get_quote_token()

                return FeesConfiguration(token, fee_rates)

    def symmetrical_rebalance(self, market: UniLpMarket, init_price: Decimal):
        if self.fees_configuration:
            year = market.market_status.timestamp.year
            fee = self.get_fee_rate_for_year(year).swap_in
            self.substract_gas_fee(fee)
            self.stats.rebalances_cost += fee

        logging.info(f"({market.market_status.timestamp}) Rebalancing funds...")
        market.even_rebalance(init_price)
        self.stats.rebalances_count += 1

    def asymmetrical_rebalance(self, market: UniLpMarket, init_price: Decimal):
        if self.fees_configuration:
            year = market.market_status.timestamp.year
            fee = self.get_fee_rate_for_year(year).swap_in
            self.substract_gas_fee(fee)
            self.stats.rebalances_cost += fee

        logging.info(f"({market.market_status.timestamp}) Rebalancing funds...")
        market_asymmetric_rebalance(market, init_price)
        self.stats.rebalances_count += 1

    def add_liquidity(self, market: UniLpMarket, range: Decimal,
                      base_max_amount: Decimal | float = None,
                      quote_max_amount: Decimal | float = None,
                      rebalance_direction: RebalanceDirection = None,
                      init_price: Decimal = None) -> (PositionInfo, Decimal, Decimal, int):
        init_price = market.market_status.price if init_price is None else init_price
        lower_quote_price, upper_quote_price = get_lower_upper_price_for_symmetric_range(range, init_price)

        new_position, base_token_used, quote_token_used, liquidity = self.add_liquidity_custom_range(market,
                                                                                                     lower_quote_price,
                                                                                                     upper_quote_price,
                                                                                                     base_max_amount,
                                                                                                     quote_max_amount,
                                                                                                     rebalance_direction)

        # custom manual property used in strategy D
        market.positions[new_position].range = range

        return new_position, base_token_used, quote_token_used, liquidity

    def add_liquidity_custom_range(self, market: UniLpMarket, lower_quote_price: Decimal, upper_quote_price: Decimal,
                                   base_max_amount: Decimal | float = None,
                                   quote_max_amount: Decimal | float = None,
                                   rebalance_direction: RebalanceDirection = None) -> (
            PositionInfo, Decimal, Decimal, int):
        base_spent_on_fees, quote_spent_on_fees = Decimal(0), Decimal(0)
        if self.fees_configuration:
            year = market.market_status.timestamp.year
            fee = self.get_fee_rate_for_year(year).liquidity_providing
            base_spent_on_fees, quote_spent_on_fees = self.substract_gas_fee(
                self.get_fee_rate_for_year(year).liquidity_providing)
            self.stats.providing_lp_cost += fee

        base_holdings, quote_holdings = self.get_base_and_quote_holdings()

        new_position, base_token_used, quote_token_used, liquidity = market.add_liquidity(lower_quote_price,
                                                                                          upper_quote_price,
                                                                                          base_max_amount=None if base_max_amount is None else base_max_amount - base_spent_on_fees,
                                                                                          quote_max_amount=None if quote_max_amount is None else quote_max_amount - quote_spent_on_fees)
        self.stats.providing_lp_count += 1
        timestamp = market.market_status.timestamp
        self.on_successful_position_opened(timestamp, range_min=float(lower_quote_price),
                                           range_max=float(upper_quote_price),
                                           rebalance_direction=rebalance_direction)



        logging.info(
            f"({timestamp}) Opened position {self.position_to_string(new_position)} range"
            f" {lower_quote_price:.4f}-{upper_quote_price:.4f} "
            f" market quote token price =  {self.get_current_default_quote_token_market_price():.4f} "
            f"{self.get_base_token().name} spent: {base_token_used:.4f}, {self.get_quote_token().name} "
            f"spent {quote_token_used:.4f} liquidity: {liquidity} "
            f" Overall base token used (used/total) = {base_token_used + quote_token_used * self.get_current_default_quote_token_market_price():.2f}/{base_holdings + (quote_holdings * self.get_current_default_quote_token_market_price()):.2f}"
        )


        self.print_assets()
        return new_position, base_token_used, quote_token_used, liquidity

    def remove_all_liquidity(self, market: UniLpMarket):
        if len(market.positions) < 1:
            return

        keys = list(market.positions.keys())
        for position_key in keys:
            self.remove_liquidity(market, position_key)

    def remove_liquidity(self, market: UniLpMarket, position: PositionInfo, liquidity: int = None):
        base_received, quote_received = market.remove_liquidity(position, liquidity)
        self.stats.withdrawing_lp_count += 1

        if self.fees_configuration:
            year = market.market_status.timestamp.year
            fee = self.get_fee_rate_for_year(year).removing_liquidity
            self.substract_gas_fee(fee)
            self.stats.withdrawing_lp_cost += fee

        logging.info(
            f"({market.market_status.timestamp}) "
            f"{'Closed' if liquidity is None else 'Removed ' + str(liquidity) + ' liquidity from'} position "
            f"{self.position_to_string(position)} and got back "
            f"{base_received:.2f} {self.get_base_token().name} {quote_received:.2f} {self.get_quote_token().name}")

    def substract_gas_fee(self, fee_amount: Decimal) -> [Decimal, Decimal]:
        fee_token = self.fees_configuration.token
        fee_token_balance = self.broker.assets[fee_token].balance
        timestamp = self.markets.default.market_status.timestamp
        is_fee_token_base_token = fee_token == self.get_base_token()

        if fee_token_balance >= fee_amount:
            logging.info(f"({timestamp}) Substracting {fee_amount} {fee_token.name} gas fee. ")
            return self.deduct_fee_from_balance(fee_token, fee_amount, is_fee_token_base_token)
        else:
            return self.substract_gas_fee_on_insufficient_fee_token_amount(fee_token, fee_amount, timestamp,
                                                                           is_fee_token_base_token)

    def deduct_fee_from_balance(self, token: TokenInfo, amount: Decimal, is_fee_token_base_token: bool) -> [Decimal,
                                                                                                            Decimal]:
        self.broker.assets[token].balance -= amount

        base_substracted, quote_substracted = Decimal(0), Decimal(0)

        if is_fee_token_base_token:
            base_substracted = amount
        else:
            quote_substracted = amount

        return base_substracted, quote_substracted

    def substract_gas_fee_on_insufficient_fee_token_amount(self, fee_token: TokenInfo, fee_amount: Decimal, timestamp,
                                                           is_fee_token_base_token: bool) -> [
        Decimal, Decimal]:
        current_price = self.markets.default.market_status.price
        available_fee_token_balance = self.broker.assets[fee_token].balance
        remaining_fee_to_be_paid = fee_amount - available_fee_token_balance

        if is_fee_token_base_token:
            quote_token_spent = self.pay_remaining_fee_in_quote_token(fee_token, remaining_fee_to_be_paid,
                                                                      current_price,
                                                                      timestamp)

            return available_fee_token_balance, quote_token_spent
        else:
            base_token_spent = self.pay_remaining_fee_in_base_token(fee_token, remaining_fee_to_be_paid,
                                                                    current_price,
                                                                    timestamp)

            return base_token_spent, available_fee_token_balance

    def pay_remaining_fee_in_quote_token(self, fee_token, amount, current_price, timestamp) -> Decimal:
        fee_to_be_paid_in_quote_token = amount / current_price
        available_amount_of_quote_token = self.broker.assets[self.get_quote_token()].balance

        if available_amount_of_quote_token >= fee_to_be_paid_in_quote_token:
            self.broker.assets[self.get_quote_token()].balance -= fee_to_be_paid_in_quote_token
            self.broker.assets[fee_token].balance = 0

            logging.info(f"({timestamp}) Paid {amount} {fee_token.name} and "
                         f"{fee_to_be_paid_in_quote_token} {self.get_quote_token().name} gas fees. ")

            return fee_to_be_paid_in_quote_token
        else:
            raise Exception(f"({timestamp}) Ran out of funds to pay gas fees!")

    def pay_remaining_fee_in_base_token(self, fee_token, amount, current_price, timestamp) -> Decimal:
        fee_to_be_paid_in_base_token = amount * current_price
        available_amount_of_base_token = self.broker.assets[self.get_base_token()].balance

        if available_amount_of_base_token >= fee_to_be_paid_in_base_token:
            self.broker.assets[self.get_base_token()].balance -= fee_to_be_paid_in_base_token
            self.broker.assets[fee_token].balance = 0

            logging.info(f"({timestamp}) Paid {amount} {fee_token.name} and "
                         f"{fee_to_be_paid_in_base_token} {self.get_base_token().name} gas fees. ")

            return fee_to_be_paid_in_base_token
        else:
            raise Exception(f"({timestamp}) Ran out of funds to pay gas fees!")

    def get_fee_rate_for_year(self, year: int) -> Fee:
        fee_rates = self.fees_configuration.fee_rates
        configuration = fee_rates.get(year, None)

        if not configuration:
            configuration = fee_rates[max(fee_rates.keys())]

        return configuration

    def get_average_max_daily_price_change(self, last_days_count: int, current_timestamp: Timestamp):
        price_moves = []
        for i in range(1, last_days_count + 1):
            timestamp_to_consider = current_timestamp - timedelta(days=i - 1)

            price_change_in_percentage = self.get_max_price_change_in_past_period(timestamp_to_consider,
                                                                                  timedelta(days=1))
            price_moves.append(float(price_change_in_percentage))

        return statistics.mean(price_moves)

    def get_current_default_quote_token_market_price(self) -> Decimal:
        return self.markets.default.market_status.price

    def get_current_default_base_token_market_price(self) -> Decimal:
        return 1 / self.markets.default.market_status.price

    def get_position(self, position_info: PositionInfo) -> Position:
        return self.markets.default.positions[position_info]

    def get_positions(self) -> Dict[PositionInfo, Position]:
        return self.markets.default.positions

    def get_market(self) -> UniLpMarket:
        return self.markets.default
