import json
import logging
import math
from _decimal import Decimal
from typing import List
import pandas as pd
from matplotlib.pylab import plt
import matplotlib.dates as mdates

from demeter import MarketInfo, TokenInfo
from demeter.broker import AccountStatus


def plotter(account_status_list: List[AccountStatus]):
    net_value_ts = [status.net_value for status in account_status_list]
    time_ts = [status.timestamp for status in account_status_list]
    plt.plot(time_ts, net_value_ts)
    plt.show()


def generate_general_info_plot(account_status: pd.DataFrame, base_token: TokenInfo,
                               quote_token: TokenInfo,
                               price: pd.Series, market: MarketInfo, result_path,
                               number_of_days: int, custom_lines: [object],
                               multi_position=False):
    price_margin_percentage = 5
    plt.rcParams.update({'font.size': 6})
    fig, value_ax = plt.subplots(dpi=400)

    max_amount_of_days_on_plot = 19

    day = mdates.DayLocator(interval=math.ceil(number_of_days / max_amount_of_days_on_plot))

    price_ax = value_ax.twinx()
    price_ax.xaxis.set_major_locator(day)
    price_ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m'))
    value_ax.set_xlabel('time')
    value_ax.set_ylabel(f'value ({base_token.name})', color='g')
    price_ax.set_ylabel(f'price ({base_token.name})', color='b')

    net_value_ts = list(account_status.net_value)
    time_ts = list(account_status.index)
    price_ts = list(price)

    value_in_position = account_status[market.name + "_net_value"]
    value_in_account = account_status[base_token.name] + account_status[quote_token.name] * price

    value_ax.plot(time_ts, net_value_ts, 'g-', label="net value", linewidth=0.3)
    value_ax.plot(time_ts, value_in_position, 'r-', label="value in position", linewidth=0.3)
    value_ax.plot(time_ts, value_in_account, 'b-', label="value in broker account", linewidth=0.3)
    price_ax.plot(time_ts, price_ts, 'y-', label="price", linewidth=0.5)

    price_lim_low: Decimal = min(price_ts)
    price_lim_high: Decimal = max(price_ts)

    for custom_line in custom_lines:
        if custom_line['ax'] == 'price':
            ax = price_ax
            price_lim_high = Decimal(max(price_lim_high, custom_line['data'].max(0)))
            price_lim_low = Decimal(min(price_lim_low, custom_line['data'].min(0)))
        elif custom_line['ax'] == 'value':
            ax = value_ax
        else:
            raise Exception("Ax not correctly set!")

        ax.plot(time_ts, custom_line['data'], custom_line['color'],
                label=custom_line['label'] if "label" in custom_line else None,
                linewidth=custom_line.get('linewidth', 0.3),
                alpha=0.7)

    price_ax.set_ylim(price_lim_low * Decimal(((100 - price_margin_percentage) * 0.01)),
                      price_lim_high * Decimal((100 + price_margin_percentage) * 0.01))

    fig.legend()
    fig.show()
    fig.savefig(result_path + ("/multi-position-plot.png" if multi_position else "/plot.png"), bbox_inches="tight")


def generate_lp_operations_plot(rebalances: pd.Series, lp_providing: pd.Series, lp_withdrawing: pd.Series,
                                account_status: pd.DataFrame, result_path: str,
                                number_of_days: int):
    plt.rcParams.update({'font.size': 6})
    fig, lp_stats_ax = plt.subplots(dpi=300)

    max_amount_of_days_on_plot = 19

    day = mdates.DayLocator(interval=math.ceil(number_of_days / max_amount_of_days_on_plot))

    lp_stats_ax.xaxis.set_major_locator(day)
    lp_stats_ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m'))
    lp_stats_ax.set_xlabel('time')
    lp_stats_ax.set_ylabel('rebalances', color='g')

    time_ts = list(account_status.index)

    lp_stats_ax.plot(time_ts, rebalances, 'g-', label="Rebalances", alpha=0.7)
    lp_stats_ax.plot(time_ts, lp_providing, 'b:', label="LP Providing", alpha=0.7)
    lp_stats_ax.plot(time_ts, lp_withdrawing, 'r-', label="LP Withdrawing", alpha=0.7)

    fig.legend()
    fig.show()
    fig.savefig(result_path + "/plot-rebalances.png", bbox_inches="tight")


def generate_fees_plot(base_fees: pd.Series, quote_fees: pd.Series, base_token: TokenInfo, quote_token: TokenInfo,
                       account_status: pd.DataFrame, result_path: str,
                       number_of_days: int):
    plt.rcParams.update({'font.size': 6})
    fig, base_ax = plt.subplots(dpi=300)

    max_amount_of_days_on_plot = 19

    day = mdates.DayLocator(interval=math.ceil(number_of_days / max_amount_of_days_on_plot))

    quote_ax = base_ax.twinx()
    quote_ax.xaxis.set_major_locator(day)
    quote_ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m'))
    base_ax.set_xlabel('time')
    base_ax.set_ylabel(base_token.name, color='g')
    quote_ax.set_ylabel(quote_token.name, color='g')

    time_ts = list(account_status.index)

    base_ax.plot(time_ts, base_fees, 'g-', label=f"{base_token.name} fees earned")
    quote_ax.plot(time_ts, quote_fees, 'r:', label=f"{quote_token.name} fees earned")

    fig.legend()
    fig.show()
    fig.savefig(result_path + "/plot-fees.png", bbox_inches="tight")


def generate_portfolio_chart(account_status: [], hodl_value: [], pool_addr: str, result_path: str):
    filtered_status = list(
        filter(lambda s: s.timestamp.minute == 0 and s.timestamp.second == 0, account_status))  # every full hour
    chart = []
    starting_value = 1000  # first price point on chart
    price_multiplier = starting_value / filtered_status[0].net_value
    hodl_multiplier = starting_value / hodl_value[filtered_status[0].timestamp]

    for entry in filtered_status:
        timestamp_mills = int(entry.timestamp.timestamp() * 1000)
        price = float(f"{(entry.net_value * price_multiplier):.6f}")
        hodl_price = float(f"{(hodl_value[entry.timestamp] * hodl_multiplier):.6f}")
        chart.append([timestamp_mills, price, hodl_price])

    logging.info("saving chart for backend purposes")
    json_obj = json.dumps({pool_addr: chart})

    with open(f"{result_path}portfolio-chart.json", "w") as outfile:
        outfile.write(json_obj)
