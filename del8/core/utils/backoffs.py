"""TODO: Add title."""
import functools
import random
import time
from absl import logging


def linear_to_exp_backoff(
    exceptions_to_catch=(),
    should_retry_on_exception_fn=lambda e: False,
    *,
    # These just provide the default values. You can override on each function
    # call if needed.
    linear_backoff_steps=5,
    linear_interval_secs=5,
    exp_backoff_steps=4,
    exp_start_interval_secs=5,
    exp_backoff_base=2,
    backoff_noise_ms=50,
):
    exceptions_to_catch = tuple(exceptions_to_catch)

    def decorator(fn):
        @functools.wraps(fn)
        def inner(
            *args,
            # The `no_backoff` arg is useful when we put this wrapper on a
            # function that calls another function with its own backoff and
            # don't want to double up the backoffs while being able to control
            # the backoffs via passed arguments for the outer function.
            no_backoff=False,
            linear_backoff_steps=linear_backoff_steps,
            linear_interval_secs=linear_interval_secs,
            exp_backoff_steps=exp_backoff_steps,
            exp_start_interval_secs=exp_start_interval_secs,
            exp_backoff_base=exp_backoff_base,
            backoff_noise_ms=backoff_noise_ms,
            **kwargs,
        ):
            if no_backoff:
                intervals = []
            else:
                intervals = [
                    linear_interval_secs for i in range(linear_backoff_steps)
                ] + [
                    exp_start_interval_secs * exp_backoff_base ** i
                    for i in range(exp_backoff_steps)
                ]

            attempts = 0
            total_secs_waited = 0

            while True:
                try:
                    return fn(*args, **kwargs)

                except exceptions_to_catch as e:
                    if not should_retry_on_exception_fn(e):
                        raise e

                    attempts += 1
                    if not intervals:
                        msg = (
                            f"Unable to call {fn.__name__} with an acceptable outcome. "
                            f"We had a total of {attempts} attempts with a cumulative "
                            f"total of {total_secs_waited} seconds waited between attempts."
                        )
                        if not no_backoff:
                            logging.warning(msg)
                        raise e

                    interval = intervals.pop(0)
                    total_secs_waited += interval

                    noise = (random.random() - 0.5) * backoff_noise_ms / 1000
                    time.sleep(interval + noise)

            raise Exception("If we get here, then there is a bug in the code.")

        return inner

    return decorator
