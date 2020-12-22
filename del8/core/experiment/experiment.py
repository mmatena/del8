"""TODO: Add title."""
import abc

from .. import data_class
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
                p_key_val.append(p[k])
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


def experiment(
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
        if not _are_keys_unique(key_fields, varying_params):
            raise ValueError(
                f"The key fields for {cls.__name__} do not lead to unique keys given the varying params."
            )

        @dec_util.wraps_class(cls)
        class Experiment(cls, _ExperimentABC):
            def __init__(self):
                self.uuid = uuid

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

            def get_full_parameters_list(self):
                params_cls = self.params_cls
                params = []
                for varying in self.varying_params:
                    params.append(params_cls(**self.fixed_params, **varying))
                return params

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
                    "group_cls": self.group.__class__,
                    "experiment_cls": self.__class__,
                    "run_uuid": storage_params.get_storage_cls().new_uuid(),
                    "executable_cls": self.executable_cls,
                    "init_kwargs": config.init_kwargs,
                    "call_kwargs": config.call_kwargs,
                    "run_params": params,
                }
                return executor.ExecutionItem(
                    worker_run_kwargs=run_kwargs,
                )

            def create_run_key_values(self, params):
                return {k: getattr(params, k) for k in self.key_fields}

            def create_all_execution_items(self):
                return [
                    self.create_execution_item(p)
                    for p in self.get_full_parameters_list()
                ]

        # Return a singleton instance.
        exp = Experiment()
        group.add_experiment(exp)
        return exp

    return dec
