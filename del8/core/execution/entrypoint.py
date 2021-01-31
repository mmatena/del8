"""TODO: Add title."""
from del8.core.di import executable
from del8.core.di import scopes
from del8.core.storage.storage import RunState

from del8.core.experiment import runs


@executable.executable()
def save_params_at_run_start(run_params, storage, experiment, run_uuid):
    run_key = runs.RunKey(
        # NOTE: Not sure if the uuid should be here as they are stored in the
        # same row as the serialized data.
        experiment_uuid=experiment.uuid,
        run_uuid=run_uuid,
        key_values=experiment.create_run_key_values(run_params),
    )

    storage.store_item(run_key)
    storage.store_item(run_params)


@executable.executable()
def set_run_state(state, storage):
    storage.set_run_state(state)


def worker_run(
    *,
    global_binding_specs,
    storage_params,
    group_cls,
    experiment_cls,
    executable_cls,
    init_kwargs=None,
    call_kwargs=None,
    # run_uuid=None,
    preload_blob_uuids=None,
    # The run_params are used purely for storage at the start of the experiment
    # and do not affect any execution.
    run_params=None,
):
    # NOTE: Should only be called on the worker. Users probably won't call
    # this method directly.
    if not init_kwargs:
        init_kwargs = {}
    if not call_kwargs:
        call_kwargs = {}

    # if not run_uuid:
    #     run_uuid = storage_params.get_storage_cls().new_uuid()
    run_uuid = storage_params.get_storage_cls().new_uuid()

    # Due to the class decorators returning an instance, we should not
    # call these.
    group = group_cls
    experiment = experiment_cls

    extra_global_binding_specs = [
        scopes.ArgNameBindingSpec("group", group),
        scopes.ArgNameBindingSpec("experiment", experiment),
        scopes.ArgNameBindingSpec("run_uuid", run_uuid),
    ]

    total_global_binding_specs = list(global_binding_specs) + extra_global_binding_specs

    with scopes.binding_scope(total_global_binding_specs):
        with storage_params.instantiate_storage() as storage:

            if preload_blob_uuids and storage.can_preload_blobs():
                storage.preload_blobs(preload_blob_uuids)

            # NOTE: I might want to avoid injecting storage directly and instead mediate
            # interactions with storage via injected instances of ExperimentGroup, Experiment,
            # and Procedure.
            #
            # I'd need to think how re-usable executables that interact with storage such as
            # the checkpoint saver would work in that framework, though.
            with scopes.binding_by_name_scope("storage", storage):
                set_run_state()(RunState.STARTED)
                if run_params:
                    save_params_at_run_start()(run_params)
                executable_cls(**init_kwargs)(**call_kwargs)
                set_run_state()(RunState.FINISHED)
