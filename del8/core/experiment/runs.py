"""TODO: Add title."""
from .. import data_class


@data_class.data_class()
class RunKey(object):
    def __init__(self, experiment_uuid, run_uuid, key_values):
        # The `key_values` will be a dict of the values of the experiment
        # `key_fields` for the particular run.
        pass


@data_class.data_class()
class RunInstanceConfig(object):
    # NOTE: Used temporarily for constructing runs from experiments.
    def __init__(self, global_binding_specs, init_kwargs=None, call_kwargs=None):
        if not self.init_kwargs:
            self.init_kwargs = {}
        if not self.call_kwargs:
            self.call_kwargs = {}
