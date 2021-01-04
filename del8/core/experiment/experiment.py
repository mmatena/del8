"""TODO: Add title."""
import abc
import contextlib

from absl import logging

from del8.core.di import scopes
from del8.core.storage.storage import RunState
from del8.executables.models import checkpoints

from . import runs
from .. import data_class
from .. import serialization
from ..di import dependencies
from ..execution import executor
from ..utils import decorator_util as dec_util
from ..utils import type_util


def _are_keys_unique(key_fields, varying_params):
    # NOTE: This does not take into account missing fields
    # and their defaults in the params class. That should
    # actually be possible to do if we pass the params class
    # to this function, I'll leave out that edge-case in the
    # first pass.
    key_fields = list(key_fields)
    values = []
    for p in varying_params:
        p_key_val = []
        for k in key_fields:
            if k in p:
                v = p[k]
                if isinstance(v, dict):
                    v = type_util.hashabledict(v)
                elif isinstance(v, list):
                    v = tuple(v)
                p_key_val.append(v)
        values.append(tuple(p_key_val))
    return len(values) == len(set(values))


def _default_key_fields(varying_params):
    key_fields = set()
    for p in varying_params:
        key_fields |= set(p.keys())
    return key_fields


class _ExperimentABC(abc.ABC):
    @abc.abstractmethod
    def create_run_instance_config(self, params):
        raise NotImplementedError

    def create_preload_blob_uuids(self, params):
        return None


def is_experiment(obj):
    return isinstance(obj, _ExperimentABC)


def experiment(  # noqa: C901
    *,
    uuid,
    group,
    varying_params,
    params_cls,
    executable_cls,
    #
    fixed_params=None,
    key_fields=None,
    #
    name="",
    description="",
    #
    # Optional sequence of binding specs.
    # NOTE: Figure out a good way to decide which bindings go here and which
    # go in the fixed params.
    bindings=(),
    #
    extra_apt_get_packages=(),
    extra_pip_packages=(),
    extra_project_params=(),
    #
    # NOTE: These are an optional override. Otherwise, the group's storage
    # params will be used instead.
    storage_params=None,
):
    if not fixed_params:
        fixed_params = {}
    if not key_fields:
        key_fields = _default_key_fields(varying_params)
    # Ensure that we are dealing with a set.
    key_fields = set(key_fields)

    def dec(cls):
        @dec_util.wraps_class(cls)
        class Experiment(cls, _ExperimentABC):
            def __init__(self):
                self.uuid = uuid
                self._true_uuid = uuid

                self.name = name
                self.description = description

                self.group = group

                self.params_cls = params_cls
                self.executable_cls = executable_cls

                self.fixed_params = fixed_params
                self.varying_params = varying_params
                self.key_fields = key_fields

                self.bindings = bindings

                self.extra_apt_get_packages = extra_apt_get_packages
                self.extra_pip_packages = extra_pip_packages
                self.extra_project_params = extra_project_params

                self.storage_params = storage_params

                self._storage = None

            def get_full_parameters_list(self, skip_finished=True):
                params_cls = self.params_cls
                params = []

                with self.get_storage():
                    if callable(self.varying_params):
                        self.varying_params = self.varying_params(self)
                    varying_params = self.varying_params

                    if not _are_keys_unique(key_fields, varying_params):
                        raise ValueError(
                            f"The key fields for {cls.__name__} do not lead to unique keys given the varying params."
                        )

                    for varying in varying_params:
                        p = params_cls(**self.fixed_params, **varying)
                        if skip_finished:
                            finished_run_uuids = self.has_run_been_completed(p)
                            if finished_run_uuids:
                                run_key_values = self.create_run_key_values(p)
                                uuids_str = ", ".join(finished_run_uuids)
                                logging.info(
                                    f"Skipping parameters with run key {run_key_values} due to presence of "
                                    f"completed runs with uuids {{{uuids_str}}}."
                                )
                                continue
                        params.append(p)
                return params

            def has_run_been_completed(self, params):
                with self.get_storage() as storage:
                    run_key_values = self.create_run_key_values(params)
                    run_keys = storage.run_keys_from_partial_values(
                        group_uuid=self.group.uuid,
                        experiment_uuid=self.uuid,
                        run_key_values=run_key_values,
                    )
                    finished_run_uuids = []
                    for run_key in run_keys:
                        try:
                            run_state = storage.get_run_state(run_key.run_uuid)
                        except ValueError:
                            # This happens when we don't have a run-key, which
                            # shouldn't happen now. So this is just here for back
                            # compatability until I clean out the DB.
                            continue
                        if run_state == RunState.FINISHED:
                            finished_run_uuids.append(run_key.run_uuid)
                return finished_run_uuids

            def get_all_package_kwargs(self, binding_specs):
                exe_classes = dependencies.get_all_executables_classes_in_graph(
                    self.executable_cls, binding_specs
                )
                apt_get = set(dependencies.get_all_apt_get_packages(exe_classes))
                pip = set(dependencies.get_all_pip_packages(exe_classes))

                apt_get |= set(self.extra_apt_get_packages) | set(
                    self.group.extra_apt_get_packages
                )
                pip |= set(self.extra_pip_packages) | set(self.group.extra_pip_packages)

                return {"apt_get_packages": list(apt_get), "pip_packages": list(pip)}

            def get_all_project_params(self):
                gpp = type_util.ensure_iterable(self.group.project_params)
                epp = type_util.ensure_iterable(self.extra_project_params)
                return list(gpp) + list(epp)

            def get_storage_params(self):
                if not self.storage_params:
                    return self.group.storage_params
                return self.storage_params

            def create_execution_item(self, params):
                config = self.create_run_instance_config(params)
                storage_params = self.get_storage_params()

                binding_specs = tuple(config.global_binding_specs)
                binding_specs += tuple(self.bindings)
                binding_specs += tuple(self.group.groupwide_bindings)

                run_kwargs = {
                    "global_binding_specs": binding_specs,
                    "storage_params": storage_params,
                    # Note that these will be serialized as their class.
                    "group_cls": self.group,
                    "experiment_cls": self,
                    "run_uuid": storage_params.get_storage_cls().new_uuid(),
                    "executable_cls": self.executable_cls,
                    "init_kwargs": config.init_kwargs,
                    "call_kwargs": config.call_kwargs,
                    "preload_blob_uuids": self.create_preload_blob_uuids(params),
                    "run_params": params,
                }
                return executor.ExecutionItem(
                    worker_run_kwargs=run_kwargs,
                )

            def create_run_key_values(self, params):
                return {k: getattr(params, k) for k in self.key_fields}

            def create_all_execution_items(self, skip_finished=True):
                return [
                    self.create_execution_item(p)
                    for p in self.get_full_parameters_list(skip_finished=skip_finished)
                ]

            def as_json(self):
                return serialization.serialize_class(self.__class__)

            def get_storage(self):
                if not self._storage:
                    storage_params = self.get_storage_params()
                    self._storage = storage_params.instantiate_storage(
                        group=self.group, experiment=self
                    )
                return self._storage

            def retrieve_run_uuids(self, run_state=None):
                with self.get_storage() as storage:
                    return storage.retrieve_run_uuids(
                        group_uuid=self.group.uuid,
                        experiment_uuid=self.uuid,
                        run_state=run_state,
                    )

            def retrieve_single_item_by_class(self, item_cls, run_uuid):
                with self.get_storage() as storage:
                    return storage.retrieve_single_item_by_class(
                        item_cls=item_cls,
                        group_uuid=self.group.uuid,
                        experiment_uuid=self.uuid,
                        run_uuid=run_uuid,
                    )

            def retrieve_items_by_class(self, item_cls, run_uuid):
                with self.get_storage() as storage:
                    return storage.retrieve_items_by_class(
                        item_cls=item_cls,
                        group_uuid=self.group.uuid,
                        experiment_uuid=self.uuid,
                        run_uuid=run_uuid,
                    )

            def retrieve_run_params(self, run_uuid):
                return self.retrieve_single_item_by_class(self.params_cls, run_uuid)

            def retrieve_run_key(self, run_uuid):
                return self.retrieve_single_item_by_class(runs.RunKey, run_uuid)

            def retrieve_checkpoints_summary(self, run_uuid):
                return self.retrieve_single_item_by_class(
                    checkpoints.CheckpointsSummary, run_uuid
                )

            def to_dev_mode(self):
                prefix = "__dev__"
                self.uuid = prefix + self._true_uuid[: -len(prefix)]

        # Return a singleton instance.
        exp = Experiment()
        group.add_experiment(exp)
        return exp

    return dec


def with_experiment_storages():
    def dec(fn):
        def inner(*args, **kwargs):
            all_args = list(args) + list(kwargs.values())
            storages = [arg.get_storage() for arg in all_args if is_experiment(arg)]
            with scopes.multiple(*storages):
                return fn(*args, **kwargs)

        return inner

    return dec
