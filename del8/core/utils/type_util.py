"""TODO: Add title."""


def islambda(possible_lambda):
    """Sees if a variable is lambda function."""
    return callable(possible_lambda) and possible_lambda.__name__ == "<lambda>"


def isiterable(possible_iterable):
    # NOTE: This might not be the best way to check for iterables. Google
    # it for more details.
    if isinstance(possible_iterable, str):
        # My main use of this is to convert non-lists to singleton lists, so
        # I want to be able to have lists of strings.
        return False
    return isinstance(possible_iterable, (list, tuple, set))


def ensure_iterable(possible_iterable):
    if isiterable(possible_iterable):
        return possible_iterable
    else:
        return [possible_iterable]


def _ensure_hashable(x):
    if isinstance(x, list):
        return tuple(x)
    return x


class hashabledict(dict):
    def __hash__(self):
        return hash(
            (
                frozenset(_ensure_hashable(k) for k in self.keys()),
                frozenset(_ensure_hashable(v) for v in self.values()),
            )
        )


def get_args(generic_type):
    return generic_type.__args__
