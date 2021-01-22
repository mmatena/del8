"""TODO: Add title."""

from del8.core import data_class
from del8.core.di import executable
from del8.core.di import scopes


@executable.executable()
def fit_kwargs_provider(steps_per_epoch, epochs, callbacks=None):
    # NOTE: I might provide a little more infrastructure for injecting callbacks here.
    # I think, in general, we need specific infrastructure to make injecting lists
    # (or other collections) of injectables/executables a lot nicer.
    kwargs = {
        "steps_per_epoch": steps_per_epoch,
        "epochs": epochs,
    }
    if callbacks:
        if not isinstance(callbacks, (list, tuple)):
            callbacks = [callbacks]
        kwargs["callbacks"] = callbacks
    return kwargs


@data_class.data_class()
class TrainingHistory(object):
    def __init__(self, history, run_extra_identifier=None):
        # The run_extra_identifier should probably be left blank unless we are calling
        # model.fit(...) multiple times within a single run. Then we should use
        # it to distinguish between those model.fit(...) calls.
        pass


@executable.executable()
def train_history_saver(
    history, storage, run_extra_identifier=None, _train_history_processor=None
):
    # The run_extra_identifier should probably be left blank unless we are calling
    # model.fit(...) multiple times within a single run. Then we should use
    # it to distinguish between those model.fit(...) calls.
    if _train_history_processor:
        history = _train_history_processor(history)
    else:
        history = history.history

    item = TrainingHistory(history=history, run_extra_identifier=run_extra_identifier)
    # Returns blob uuid.
    return storage.store_item(item)


@executable.executable(
    default_bindings={
        "fit_kwargs": fit_kwargs_provider,
        "train_history_saver": train_history_saver,
    },
)
class training_run(object):
    def dataset_scope(
        self,
        split,
        train_num_examples=None,
        train_skip_examples=None,
        train_batch_size=None,
        validation_num_examples=None,
        validation_skip_examples=None,
        validation_batch_size=None,
    ):
        if split == "train":
            shuffle = repeat = True
            num_examples = train_num_examples
            batch_size = train_batch_size
            dataset_skip = train_skip_examples
        elif split == "validation":
            shuffle = repeat = False
            num_examples = validation_num_examples
            batch_size = validation_batch_size
            dataset_skip = validation_skip_examples
        else:
            raise ValueError(f"Unrecognized split {split}.")
        specs = [
            scopes.ArgNameBindingSpec("shuffle", shuffle),
            scopes.ArgNameBindingSpec("repeat", repeat),
            scopes.ArgNameBindingSpec("split", split),
        ]

        if dataset_skip:
            specs.append(scopes.ArgNameBindingSpec("dataset_skip", dataset_skip))
        if batch_size:
            specs.append(scopes.ArgNameBindingSpec("batch_size", batch_size))
        if num_examples:
            specs.append(scopes.ArgNameBindingSpec("num_examples", num_examples))

        return scopes.multiple(scopes.name_scope(split), scopes.binding_scope(specs))

    def call(
        self,
        _dataset,
        compiled_model,
        _fit_kwargs,
        _train_history_saver,
        with_validation=True,
    ):
        with self.dataset_scope("train"):
            train_ds = _dataset()

        if with_validation:
            with self.dataset_scope("validation"):
                validation_ds = _dataset()
        else:
            validation_ds = None

        with scopes.binding_by_name_scope("model", compiled_model):
            fit_kwargs = _fit_kwargs()

            # # NOTE: This can be uncommented to assist with debugging.
            # compiled_model.run_eagerly = True

            history = compiled_model.fit(
                train_ds, validation_data=validation_ds, **fit_kwargs
            )

            if _train_history_saver:
                return _train_history_saver(history)
            else:
                return None
