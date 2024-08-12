from Core import utils

configs = utils.load_configs("bot_config.toml")

strategy_configs = configs["strategies"]

for name, params in strategy_configs.items():
    for p in params:
        print(name, p)
