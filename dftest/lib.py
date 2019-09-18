def to_string(x):
    if isinstance(x, (str, )):
        return x
    elif isinstance(x, (int, )):
        return str(x)
    elif isinstance(x, (float, )):
        return str(round(x, 3))
    else:
        return str(x)