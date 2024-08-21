from .fg_strategies import MAOverlap, RSICrossover


strategy_classes = {
    "MAOverlap": MAOverlap,
    "RSICrossover": RSICrossover,
}


def create_strategy(strategy_name: str):
    strategy_class = strategy_classes.get(strategy_name)
    if not strategy_class:
        raise ValueError(f"Unknown strategy name: {strategy_name}")
    return strategy_class()
