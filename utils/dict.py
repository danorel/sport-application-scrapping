import itertools


def flatten(l2D: list[list]) -> list:
    return list(itertools.chain(*l2D))
