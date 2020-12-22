"""TODO: Add title."""
import abc
import contextlib
import os
import shutil
import tempfile
import uuid as uuidlib


class StorageParams(abc.ABC):
    # NOTE: Subclasses should also probably be @data_class.

    @abc.abstractmethod
    def instantiate_storage(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_storage_cls(self):
        raise NotImplementedError


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

    @property
    @abc.abstractmethod
    def run_uuid(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def experiment_uuid(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def group_uuid(self):
        raise NotImplementedError

    @abc.abstractmethod
    def store_item(self, item) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def replace_item(self, item_uuid, new_item):
        raise NotImplementedError

    @abc.abstractmethod
    def retrieve_item(self, uuid) -> any:
        raise NotImplementedError

    @abc.abstractmethod
    def retrieve_items_by_class(
        self,
        item_cls,
        group_uuid=None,
        experiment_uuid=None,
        run_uuid=None,
    ):
        # All of the uuids are optional are should restrict the returns
        # to only items associated with the respective group/experiment/run.
        raise NotImplementedError

    # # Not sure if we want this method.
    # def store_tensors(self, tensors) -> str:
    #     """Returns UUID."""
    #     raise NotImplementedError

    @abc.abstractmethod
    def store_model_weights(self, model) -> str:
        """Returns UUID."""
        raise NotImplementedError

    @abc.abstractmethod
    def retrieve_blob_path(self, blob_uuid):
        raise NotImplementedError

    @abc.abstractmethod
    def retrieve_blob_as_file(self, blob_uuid, dst_dir):
        """Returns filepath of the local blob file."""
        raise NotImplementedError

    @contextlib.contextmanager
    def retrieve_blob_as_tempfile(self, blob_uuid):
        temp_dir = tempfile.mkdtemp()
        file = None
        try:
            filepath = self.retrieve_blob_as_file(blob_uuid, temp_dir)
            file = open(filepath, "r")
            yield file
        finally:
            if file:
                file.close()
            os.rmtree(temp_dir)

    def initialize(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, type, value, traceback):
        self.close()
