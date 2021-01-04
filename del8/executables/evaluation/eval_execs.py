"""TODO: Add title."""
import datetime
import time

from absl import logging

import tensorflow as tf

from del8.core import data_class
from del8.core.di import executable
from del8.core.di import scopes


###############################################################################


@executable.executable()
def evaluate_model(
    compiled_model,
    dataset,
    _evaluation_results_saver,
    _evaluation_results_processor=None,
):
    results = compiled_model.evaluate(dataset, return_dict=True)
    if _evaluation_results_processor:
        results = _evaluation_results_processor(results)
    return _evaluation_results_saver(results)


###############################################################################


# def _get_task_logits(compute_task_logits, dataset, task, num_classes):
#     task_logits = tf.TensorArray(
#         tf.float32,
#         size=0,
#         dynamic_size=True,
#         infer_shape=False,
#         element_shape=tf.TensorShape([None, num_classes])
#     )
#     index = 0
#     for minibatch, _ in dataset:
#         logits = compute_task_logits(minibatch, task, training=False)
#         task_logits = task_logits.write(index, logits)
#         index += 1
#     return task_logits.concat()


def _get_task_logits(compute_task_logits, dataset, task, num_classes):
    task_logits = []
    for minibatch, _ in dataset:

        start_time = time.time()

        logits = compute_task_logits(minibatch, task, training=False)

        elapsed_seconds = time.time() - start_time
        elapsed_nice = str(datetime.timedelta(seconds=elapsed_seconds))
        logging.info(f"Minibatch eval took {elapsed_nice}")

        task_logits.append(logits)
    return tf.concat(task_logits, axis=0)


@executable.executable()
def argmax_logits(logits):
    return tf.argmax(logits, axis=-1, output_type=tf.int32)


def _handle_mnli(task):
    if task in ["mnli_matched", "mnli_mismatched"]:
        return "mnli"
    return task


@executable.executable(
    default_bindings={
        "process_task_logits": argmax_logits,
    },
)
def robust_evaluate_model(
    compiled_model,
    # Dict from task name to eval dataset. Also has "{}_labels" key with a tensor of the labels.
    robust_evaluate_dataset,
    # Dict from task name to metric or list of metrics.
    metrics_for_tasks,
    _process_task_logits,
    _evaluation_results_saver,
):
    results = {}
    items = robust_evaluate_dataset.items()
    for task, dataset in items:
        if task.endswith("_labels"):
            continue

        labels = robust_evaluate_dataset[f"{task}_labels"]

        og_task = task
        task = _handle_mnli(task)

        start_time = time.time()
        task_logits = _get_task_logits(
            compiled_model.compute_task_logits,
            dataset,
            task,
            num_classes=compiled_model.get_num_classes_for_task(task),
        )
        elapsed_seconds = time.time() - start_time
        elapsed_nice = str(datetime.timedelta(seconds=elapsed_seconds))
        logging.info(f"Evaluation took {elapsed_nice}")

        with scopes.binding_by_name_scope("task", og_task):
            prediction_outputs = _process_task_logits(task_logits)

        metrics = metrics_for_tasks[og_task]
        if not isinstance(metrics, (list, tuple)):
            metrics = [metrics]

        task_results = {}
        for metric in metrics:
            task_results.update(metric(labels, prediction_outputs, return_dict=True))

        results[og_task] = task_results

    logging.info(f"Evaluation results: {results}")

    return _evaluation_results_saver(results)


###############################################################################


@data_class.data_class()
class CheckpointEvaluationResults(object):
    def __init__(self, results, checkpoint_blob_uuid):
        pass


@executable.executable()
def checkpoint_evaluation_results_saver(results, storage, checkpoint):
    item = CheckpointEvaluationResults(results, checkpoint_blob_uuid=checkpoint)
    item_uuid = storage.store_item(item)
    return item_uuid


@executable.executable(
    default_bindings={
        "evaluation_results_saver": checkpoint_evaluation_results_saver,
        "evaluate_model": evaluate_model,
    },
)
def evaluate_from_checkpoints_summary(
    checkpoints_summary, _compiled_model, _evaluate_model, should_clear_session=True
):
    retvals = []
    for i, checkpoint_blob_uuid in enumerate(checkpoints_summary.checkpoint_uuids):
        bindings = [("checkpoint", checkpoint_blob_uuid), ("checkpoint_index", i)]
        with scopes.binding_by_name_scopes(bindings):
            compiled_model = _compiled_model()
            with scopes.binding_by_name_scope("compiled_model", compiled_model):
                retval = _evaluate_model(compiled_model)
                retvals.append(retval)
            if should_clear_session:
                tf.keras.backend.clear_session()
    return retvals
