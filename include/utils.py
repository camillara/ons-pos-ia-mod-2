def add23(x):
    """Add 23 to a number."""
    if type(x) not in [int, float]:
        raise TypeError("Only int and float are supported.")
    return x + 23