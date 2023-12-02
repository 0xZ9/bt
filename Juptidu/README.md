# Brokkr-uniswap-v3-backtest

# Overview

This project contains backtesting of our Brokkr strategies
using [zelos-demeter](https://pypi.org/project/zelos-demeter/) framework.

To run strategies, you need to download data for a specific pool and network using
`download.sh` script,
see [demeter download tutorial](https://zelos-demeter.readthedocs.io/en/latest/download_tutorial.html) for
more details.

There's predownloaded 1 month of data for `0.05% ETH-USDC` pool on Arbitrum under `/data` for development.

# Strategies
Currently 3 strategies are implemented, see their description on [Notion](https://github.com/BrokkrFinance/brokkr-uniswap-v3-backtest)