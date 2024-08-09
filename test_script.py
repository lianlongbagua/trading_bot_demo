import asyncio
import time

import schedule

from gateways.okx_gateway import OKXGateway
from Core import utils
from Core.custom_factors import *  # noqa


def load_strategies(strategy_configs):
    strategies = {}

    # load all available strategies
    for strategy in strategy_configs:
        signal_generators = []
        for param in strategy_configs[strategy]:
            sg = eval(strategy + "()")
            sg.set_params(param)
            signal_generators.append(sg)
        strategies[strategy] = signal_generators
        print(f"{len(strategies[strategy])} {strategy} strategies loaded")

    return strategies


async def get_data(gateway, instId, bars, limit):
    results = await gateway.fetch_multiple(instId, bars, limit)
    contracts = await gateway.process_multiple(results)
    return contracts


def run_get_data(gateway, instId, bars, limit):
    return asyncio.run(get_data(gateway, instId, bars, limit))


if __name__ == "__main__":
    configs = utils.load_configs("bot_config.toml")
    strategy_configs = configs["strategies"]
    strategies = load_strategies(strategy_configs)

    instrument_info = configs["instrument_info"]

    gateway = OKXGateway()
    instId = instrument_info["instId"]
    bars = instrument_info["bars"]
    limit = instrument_info["limit"]

    def job():
        contracts = run_get_data(gateway, instId, bars, limit)
        for strategy in strategies:
            final_signals = []

            for sg in strategies[strategy]:
                signal = sg.generate_signals(contracts[instId + "_" + sg.interval])[-1]
                print(f"signal: {signal} from {sg}")
                final_signals.append(signal)

            output = utils.sum_signals(final_signals)

            print(f"{strategy} total signals: {final_signals}")

            print(f"final combined {strategy} signal: {output}")

    schedule.every(1).minute.do(job)
    job()

    while True:
        schedule.run_pending()
        time.sleep(1)
