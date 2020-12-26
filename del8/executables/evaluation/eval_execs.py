"""TODO: Add title."""
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
