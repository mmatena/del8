"""TODO: Add title."""
from absl import logging
import tensorflow as tf

from del8.core import data_class
from del8.core.di import executable


###############################################################################


@data_class.data_class()
class CheckpointsSummary(object):
    def __init__(self, checkpoint_uuids=(), run_extra_identifier=None):
        # Note that checkpoint_uuids are blob uuids, not item uuids.
        #
        # The run_extra_identifier should probably be left blank unless we are calling
        # model.fit(...) multiple times within a single run. Then we should use
        # it to distinguish between those model.fit(...) calls.
        pass


###############################################################################


class _SaveCheckpointCallback(tf.keras.callbacks.Callback):
    def __init__(self, checkpoint_saver):
        super().__init__()
        self.checkpoint_saver = checkpoint_saver

    def on_train_begin(self, logs=None):
        self.checkpoint_saver.reset()

    def on_epoch_end(self, epoch, logs=None):
        logging.info("Saving checkpoint")
        self.checkpoint_saver.save_checkpoint(self.model)


@executable.executable()
class checkpoint_saver_callback:
    # NOTE: This is stateful. I should think about stateful vs stateless executables
    # and see if we should differentiate between the two in the framework (or, far
    # less likely, outright ban stateful ones).

    def __init__(self):
        self.reset()

    def reset(self, storage, run_extra_identifier=None):
        self.summary = None
        self.summary_uuid = None
        self.run_extra_identifier = run_extra_identifier
        self.storage = storage

    def save_checkpoint(self, model):
        # Note that checkpoint_uuid is the uuid of the blob, not the uuid of an item.
        checkpoint_uuid = self.storage.store_model_weights(model)
        self.add_checkpoint_to_summary(checkpoint_uuid)

    def add_checkpoint_to_summary(self, checkpoint_uuid):
        if self.summary_uuid is None:
            self.initialize_summary()

        self.summary = self.summary.copy(
            checkpoint_uuids=self.summary.checkpoint_uuids + (checkpoint_uuid,)
        )

        self.storage.replace_item(self.summary_uuid, self.summary)

    def initialize_summary(self):
        if self.summary_uuid is not None:
            raise ValueError(
                "Tried to create a new checkpoints summary but one is already "
                "associated with the checkpoint saver."
            )

        self.summary = CheckpointsSummary(
            run_extra_identifier=self.run_extra_identifier
        )
        self.summary_uuid = self.storage.store_item(self.summary)

    def call(self, storage):
        return _SaveCheckpointCallback(self)


###############################################################################


@executable.executable()
def checkpoint_loader(model, checkpoint, storage):
    # TODO: Add some kwargs for the load_weights (by_name=False, skip_mismatch=False, options=None)
    # once I support "protected" bindings.
    logging.info(f"Loading checkpoint {checkpoint}")
    with storage.retrieve_blob_as_tempfile(checkpoint) as f:
        model.load_weights(f.name)
    return model
