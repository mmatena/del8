"""TODO: Add title."""
import abc

from .. import data_class


@data_class.data_class()
class ExecutionItem(object):
    # TODO: Add some attributes here.
    def __init__(self):
        pass


class Supervisor(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def from_params(cls, executor_params):
        # The `executor_params` can be a different data_class for different
        # Supervisor subclasses.
        raise NotImplementedError

    @abc.abstractmethod
    def _run(self, execution_items):
        raise NotImplementedError

    def run(self, execution_items):
        with self:
            self._run(execution_items)

    def initialize(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        self.initialize()

    def __exit__(self, type, value, traceback):
        self.close()


class WorkerHandle(abc.ABC):
    @abc.abstractmethod
    def accept_item(self, item):
        raise NotImplementedError

    def close(self):
        pass


class WorkerLauncher(abc.ABC):
    # This what launches the workers.

    def prepare_for_launches(self):
        pass

    @abc.abstractmethod
    def launch(self) -> WorkerHandle:
        raise NotImplementedError
