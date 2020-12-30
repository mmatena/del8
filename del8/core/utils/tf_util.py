"""TODO: Add title."""
import contextlib

import tensorflow as tf


@contextlib.contextmanager
def logging_level(level):
    calling_level = tf.get_logger().level
    try:
        tf.get_logger().setLevel(level)
        yield
    finally:
        tf.get_logger().setLevel(calling_level)
