"""TODO: Add title.

A lot stuff here taken from t5 at:
https://github.com/google-research/text-to-text-transfer-transformer/blob/master/t5/evaluation/metrics.py
"""
import functools
import time

from absl import logging

import numpy as np
import scipy.stats
import sklearn.metrics
import tensorflow as tf


def _return_dict(name=None):
    def dec(fn):
        @functools.wraps(fn)
        def inner(*args, return_dict=False, **kwargs):
            metric_name = name
            if metric_name is None:
                metric_name = fn.__name__

            result = fn(*args, **kwargs)

            if return_dict:
                result = {metric_name: result}

            return result

        return inner

    return dec


def _to_numpy(fn):
    def inner(targets, predictions, *args, **kwargs):
        if isinstance(targets, tf.Tensor):
            targets = targets.numpy()
        if isinstance(predictions, tf.Tensor):
            predictions = predictions.numpy()
        return fn(targets, predictions, *args, **kwargs)

    return inner


@_return_dict("accuracy")
@_to_numpy
def accuracy(targets, predictions):
    return 100 * sklearn.metrics.accuracy_score(targets, predictions)


@_return_dict("pearson_corrcoef")
@_to_numpy
def pearson_corrcoef(targets, predictions):
    """Pearson correlation coefficient."""
    ret = 100 * scipy.stats.pearsonr(targets, predictions)[0]
    if np.isnan(ret):
        ret = 0.0
    return ret


@_return_dict("spearman_corrcoef")
@_to_numpy
def spearman_corrcoef(targets, predictions):
    """Spearman correlation coefficient."""
    ret = 100 * scipy.stats.spearmanr(targets, predictions)[0]
    if np.isnan(ret):
        ret = 0.0
    return ret


@_return_dict("matthews_corrcoef")
@_to_numpy
def matthews_corrcoef(targets, predictions):
    """Spearman correlation coefficient."""
    ret = 100 * sklearn.metrics.matthews_corrcoef(targets, predictions)
    if np.isnan(ret):
        ret = 0.0
    return ret


@_return_dict("f1")
@_to_numpy
def f1_score_with_invalid(targets, predictions):
    """Compute F1 score, but any prediction != 0 or 1 is counted as incorrect.
    Args:
    targets: np.ndarray of targets, either 0 or 1
    predictions: np.ndarray of predictions, any integer value
    Returns:
    F1 score, where any prediction != 0 or 1 is counted as wrong.
    """
    targets, predictions = np.asarray(targets), np.asarray(predictions)
    # Get indices of invalid predictions
    invalid_idx_mask = np.logical_and(predictions != 0, predictions != 1)
    # For any prediction != 0 or 1, set it to the opposite of what the target is
    predictions[invalid_idx_mask] = 1 - targets[invalid_idx_mask]
    return 100 * sklearn.metrics.f1_score(targets, predictions)


@_return_dict("f1")
@_to_numpy
def f1(targets, predictions, average="binary"):
    return 100 * sklearn.metrics.f1_score(targets, predictions, average=average)


micro_f1 = functools.partial(f1, average="micro")
macro_f1 = functools.partial(f1, average="macro")


@_return_dict("macro_f1_1")
@_to_numpy
def macro_f1_1(targets, predictions):
    precision, recall, _, _ = sklearn.metrics.precision_recall_fscore_support(
        targets, predictions
    )
    f1 = 2 * (precision * recall) / (precision + recall)
    f1 = 100 * np.mean(f1)
    return 0.0 if np.isnan(f1) else f1


@_return_dict("macro_f1_2")
@_to_numpy
def macro_f1_2(targets, predictions):
    precision, recall, _, _ = sklearn.metrics.precision_recall_fscore_support(
        targets, predictions, average="macro"
    )
    f1 = 2 * (precision * recall) / (precision + recall)
    f1 = 100 * f1
    return 0.0 if np.isnan(f1) else f1
