"""TODO: Add title."""
import abc
import uuid as uuidlib


class Storage(abc.ABC):
    # NOTE: Subclasses should also probably be @executable.

    @classmethod
    @abc.abstractmethod
    def from_params(cls, storage_params):
        # The `storage_params` can be a different data_class for different
        # Storage subclasses.
        raise NotImplementedError

    @classmethod
    def new_uuid(cls) -> str:
        return uuidlib.uuid4().hex

    @abc.abstractmethod
    def get_experiment_group_uuid(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_experiment_run_uuid(self):
        raise NotImplementedError

    @abc.abstractmethod
    def create_group(self, experiment_group):
        raise NotImplementedError

    @abc.abstractmethod
    def store_item(self, item) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def replace_item(self, item_uuid, new_item):
        raise NotImplementedError

    @abc.abstractmethod
    def store_model_weights(self, model) -> str:
        """Returns UUID."""
        raise NotImplementedError

    @abc.abstractmethod
    def retrieve_item(self, uuid) -> any:
        raise NotImplementedError

    # # Not sure if we want this method.
    # def store_tensors(self, tensors) -> str:
    #     """Returns UUID."""
    #     raise NotImplementedError

    def initialize(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        self.initialize()

    def __exit__(self, type, value, traceback):
        self.close()
