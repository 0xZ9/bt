import logging
import sys
from pathlib import Path

import pandas as pd
from demeter import UniV3Pool, Actuator, MarketInfo, UniLpMarket, EvaluatorEnum

from utils.plotting_utils import construct_multiple_position_custom_columns
from utils.ui_utils import get_pool_from_user, get_user_input
from strategy_plotter import generate_general_info_plot, generate_fees_plot, generate_lp_operations_plot, \
    generate_portfolio_chart
from web3_service import get_token_infos, get_chain_type
from web3 import Web3

# noinspection PyUnresolvedReferences
import strategy_importer  # NOTE: do not delete this file, otherwise available strategies will be empty!


def get_pool_address(data_path: str) -> str:
    p = Path(data_path)
    directories: [str] = [f.name for f in p.iterdir() if f.is_dir() and len(list(f.iterdir()))]

    available_pools = list(filter(lambda d: Web3.is_address(d), directories))

    if len(available_pools) == 0:
        raise Exception(f'No pool available in directory {data_path}')
    elif len(available_pools) == 1:
        return available_pools[0]
    else:
        picked_pool = get_pool_from_user(available_pools)

        return picked_pool


def setup_libraries(log_file_path: str):
    pd.options.display.max_columns = None
    pd.set_option('display.width', 5000)

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s - %(message)s',
                        handlers=[
                            logging.FileHandler(f'{log_file_path}logs.txt'),
                            logging.StreamHandler(stream=sys.stdout)
                        ])


if __name__ == "__main__":
    result_path = "./result/"
    data_path = "./data/"

    setup_libraries(result_path)
    pool_address = get_pool_address(data_path)
    data_path = data_path + f'{pool_address}/'

    chain = get_chain_type(pool_address)

    token0, token1 = get_token_infos(pool_address, chain)

    base_token = token1
    quote_token = token0

    logging.info(f"Pool used: {chain} - {pool_address} ({token0.name}-{token1.name})")

    user_input = get_user_input(pool_address, data_path, base_token)
    start_date = user_input[0]
    end_date = user_input[1]
    starting_base_token_amount: float = float(user_input[2])
    strategy_input = user_input[3]
    strategy = strategy_input[0](*strategy_input[1])

    market_key = MarketInfo("uniswap")

    pool = UniV3Pool(token0=token0, token1=token1, fee=0.05, base_token=base_token)

    market = UniLpMarket(market_key, pool)
    market.data_path = data_path
    market.load_data(chain=chain.name,
                     contract_addr=pool_address,
                     start_date=start_date,
                     end_date=end_date)

    actuator = Actuator()
    actuator.broker.add_market(market)
    actuator.broker.set_balance(base_token, starting_base_token_amount)
    actuator.broker.set_balance(quote_token, 0)
    strategy.chain = chain
    actuator.strategy = strategy
    actuator.set_price(market.get_price_from_data())

    actuator.run(
        evaluator=[EvaluatorEnum.ANNUALIZED_RETURNS, EvaluatorEnum.MAX_DRAW_DOWN]
    )
    actuator.save_result(result_path,
                         account=True,
                         actions=False)
    custom_columns = [
        {"label": "Range low",
         'color': "m-",
         "data": actuator.broker.markets.default.data.range_low,
         'ax': 'price'},
        {"label": "Range high",
         'color': "m-",
         "data": actuator.broker.markets.default.data.range_high,
         'ax': 'price'},
        {"label": f'Initial holdings in {base_token.name}',
         'color': "c-",
         'linewidth': 1,
         "data": actuator.broker.markets.default.data.initial_investment_value,
         'ax': 'value'}
    ]

    account_status_df = actuator.get_account_status_dataframe()
    number_of_days = (end_date - start_date).days + 1
    logging.info("Generating charts, please wait...")

    multiple_positions_custom_data = construct_multiple_position_custom_columns(strategy, base_token, actuator)

    # multiple positions plot
    generate_general_info_plot(account_status_df, base_token, quote_token,
                               actuator.token_prices[quote_token.name],
                               market_key, result_path, number_of_days, multiple_positions_custom_data,
                               multi_position=True)

    # single position plot
    generate_general_info_plot(account_status_df, base_token, quote_token,
                               actuator.token_prices[quote_token.name],
                               market_key, result_path, number_of_days, custom_columns)

    generate_lp_operations_plot(actuator.broker.markets.default.data.rebalances,
                                actuator.broker.markets.default.data.lp_providing,
                                actuator.broker.markets.default.data.lp_withdrawing, account_status_df, result_path,
                                number_of_days)

    generate_fees_plot(actuator.broker.markets.default.data.base_fees, actuator.broker.markets.default.data.quote_fees,
                       base_token, quote_token,
                       account_status_df, result_path,
                       number_of_days)
    generate_portfolio_chart(actuator.account_status,
                             actuator.broker.markets.default.data.initial_investment_value,
                             pool_address,
                             result_path)

    acc_status_before = actuator.account_status[0]
    acc_status_final = actuator.final_status
    logging.info(f"Profit {acc_status_final.net_value - acc_status_before.net_value} {base_token.name}")

    base_fees_profit = actuator.broker.markets.default.data.base_fees[-1]
    quote_fees_profit = actuator.broker.markets.default.data.quote_fees[-1]

    APR = (float(base_fees_profit) * 2 / starting_base_token_amount) / (number_of_days / 365) * 100
    logging.info(
        f"Earned {base_fees_profit} {base_token.name} and {quote_fees_profit} {quote_token.name} "
        f"in {number_of_days} days investing {starting_base_token_amount} {base_token.name}.\n"
        f"APR: {APR:.2f} %\n"
        f"(approximated - as total earned fees 2*{base_token.name} is used)")
