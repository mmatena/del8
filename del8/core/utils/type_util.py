"""TODO: Add title."""
import abc


def islambda(possible_lambda):
    """Sees if a variable is lambda function."""
    return callable(possible_lambda) and possible_lambda.__name__ == "<lambda>"


def isiterable(possible_iterable):
    # NOTE: This might not be the best way to check for iterables. Google
    # it for more details.
    return isinstance(possible_iterable, abc.Iterable)
