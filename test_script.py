from pprint import pprint

from dotenv import load_dotenv

from gateways import OKXGateway
from trader.objects import parse_position_data

load_dotenv()
gateway = OKXGateway()

# output = gateway.get_account_position_risk()
# output = gateway.get_positions(instId="BTC-USDT-SWAP")
# output = gateway.get_account_balance()
# output = gateway.get_mark_price(instId="BTC-USDT-SWAP")
# output = gateway.set_leverage(instId="BTC-USDT-SWAP", lever="100", mgnMode="isolated")
# output = gateway.get_leverage(instId="BTC-USDT", mgnMode="isolated")
# output = gateway.place_order(
#     instId="BTC-USDT-SWAP",
#     tdMode="isolated",
#     ordType="market",
#     side="buy",
#     sz="100",
# )
# output = gateway.get_account_config()
# pprint(output[0])
print(bool(""))
