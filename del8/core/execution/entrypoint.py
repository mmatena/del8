"""TODO: Add title."""
from del8.core.di import scopes


def worker_run(
    *,
    global_binding_specs,
    storage_params,
    experiment_group_uuid,
    executable_cls,
    init_kwargs=None,
    call_kwargs=None,
    experiment_run_uuid=None,
):
    # NOTE: Should only be called on the worker. Users probably won't call
    # this method directly.
    if not init_kwargs:
        init_kwargs = {}
    if not call_kwargs:
        call_kwargs = {}

    if not experiment_run_uuid:
        experiment_run_uuid = storage_params.get_storage_cls().new_uuid()

    extra_global_binding_specs = [
        scopes.ArgNameBindingSpec("experiment_group_uuid", experiment_group_uuid),
        scopes.ArgNameBindingSpec("experiment_run_uuid", experiment_run_uuid),
    ]

    total_global_binding_specs = list(global_binding_specs) + extra_global_binding_specs

    with scopes.binding_scope(total_global_binding_specs):
        with storage_params.instantiate_storage() as storage:
            with scopes.binding_by_name_scope("storage", storage):
                executable_cls(**init_kwargs)(**call_kwargs)
