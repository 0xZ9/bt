from web3 import Web3
from demeter import TokenInfo, ChainType

from config.config import uniswap_pool_abi, erc20_abi, node_urls


def get_token_infos(pool_address: str, chainType: ChainType) -> [TokenInfo, TokenInfo]:
    web3 = Web3(Web3.HTTPProvider(node_urls[chainType]))

    contract = web3.eth.contract(address=Web3.to_checksum_address(pool_address.lower()), abi=uniswap_pool_abi)

    # Get the token addresses
    token0_address = contract.functions.token0().call()
    token1_address = contract.functions.token1().call()

    # Get the token name and decimals
    token0_name = web3.eth.contract(address=token0_address, abi=erc20_abi).functions.symbol().call()
    token0_decimals = web3.eth.contract(address=token0_address, abi=erc20_abi).functions.decimals().call()
    token1_name = web3.eth.contract(address=token1_address, abi=erc20_abi).functions.symbol().call()
    token1_decimals = web3.eth.contract(address=token1_address, abi=erc20_abi).functions.decimals().call()

    return [TokenInfo(name=token0_name, decimal=token0_decimals), TokenInfo(name=token1_name, decimal=token1_decimals)]


def get_chain_type(pool_address: str) -> ChainType:
    for chainType, node_url in node_urls.items():
        web3 = Web3(Web3.HTTPProvider(node_url))
        try:
            contract = web3.eth.contract(address=Web3.to_checksum_address(pool_address.lower()), abi=uniswap_pool_abi)
            contract.functions.fee().call()
            return chainType
        except:
            pass

    raise Exception(f"Can't specify chain for a pool {pool_address}")
