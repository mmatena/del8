"""TODO: Add title."""
import abc

from ..utils import decorator_util as dec_util


def group(
    *,
    uuid,
    storage_params,
    name="",
    description="",
    # Optional sequence of binding specs.
    groupwide_bindings=(),
    extra_apt_get_packages=(),
    extra_pip_packages=(),
    project_params=(),
):
    def dec(cls):
        @dec_util.wraps_class(cls)
        class ExperimentGroup(cls):
            def __init__(self):
                self.uuid = uuid

                self.name = name
                self.description = description

                self.storage_params = storage_params
                self.groupwide_bindings = groupwide_bindings

                self.extra_apt_get_packages = extra_apt_get_packages
                self.extra_pip_packages = extra_pip_packages
                self.project_params = project_params

                self.uuid_to_experiment = {}

            @property
            def experiments(self):
                return tuple(self.uuid_to_experiment.values())

            def add_experiment(self, experiment):
                if experiment.uuid not in self.uuid_to_experiment:
                    self.uuid_to_experiment[experiment.uuid] = experiment
                else:
                    existing = self.uuid_to_experiment[experiment.uuid]
                    if existing != experiment:
                        raise ValueError(
                            f"Found two different experimets with same uuid {experiment.uuid}."
                        )

        # Return a singleton instance.
        return ExperimentGroup()

    return dec
